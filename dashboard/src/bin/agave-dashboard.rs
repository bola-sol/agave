//! The dashboard page in its own process, fed from the validator's event streams.
//!
//! When the validator it reads exits or is replaced, it starts itself again from argv[0], since
//! the mirror and the collector hold the old validator's state.

use {
    agave_dashboard::{
        admin_socket, config::DashboardConfig, event_input::EventInput, mirror::Mirror,
        service::DashboardService,
    },
    solana_core::validator::ValidatorStartProgress,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        os::unix::process::CommandExt,
        path::{Path, PathBuf},
        process::{Command, ExitCode},
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
    tokio::signal::unix::{SignalKind, signal},
};

const USAGE: &str = "\
Usage: agave-dashboard --ledger <DIR> [OPTIONS]

  --ledger <DIR>         The validator's ledger: its events/ and admin.rpc are read
  --port <PORT>          Port to serve the page on [default: 10999]
  --bind-address <HOST>  Address to serve on [default: 127.0.0.1]; any other address exposes
                         validator internals to anyone who can reach it
  --allowed-host <HOST>  A host name the page answers to besides IP addresses and localhost,
                         such as a proxy's; repeatable
  --history-dir <DIR>    Where the page's history is written and read back after a restart
  --poll-ms <MS>         How often the event streams are read [default: 5]
  --report-secs <SECS>   How often the streams' counts are logged [default: 10]
  -h, --help             Print this";

const DEFAULT_PORT: u16 = 10999;

#[derive(Debug, PartialEq, Eq)]
struct Args {
    ledger: PathBuf,
    listen_addr: SocketAddr,
    allowed_hosts: Vec<String>,
    history_dir: Option<PathBuf>,
    poll: Duration,
    report: Duration,
}

/// `None` for `--help`.
fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Args>, String> {
    let mut ledger = None;
    let mut port = DEFAULT_PORT;
    let mut bind_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let mut allowed_hosts = Vec::new();
    let mut history_dir = None;
    let mut poll_ms = 5u64;
    let mut report_secs = 10u64;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        // `--flag=value` as well as `--flag value`.
        let (flag, mut inline) = match arg.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => {
                (flag.to_string(), Some(value.to_string()))
            }
            _ => (arg, None),
        };
        let mut value = |name: &str| {
            inline
                .take()
                .or_else(|| args.next().filter(|next| !next.starts_with("--")))
                .ok_or(format!("{name} needs a value"))
        };
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--ledger" => ledger = Some(PathBuf::from(value("--ledger")?)),
            "--port" => {
                port = value("--port")?
                    .parse()
                    .map_err(|err| format!("--port: {err}"))?
            }
            "--bind-address" => {
                bind_address = solana_net_utils::parse_host(&value("--bind-address")?)
                    .map_err(|err| format!("--bind-address: {err}"))?
            }
            "--allowed-host" => allowed_hosts.push(value("--allowed-host")?),
            "--history-dir" => history_dir = Some(PathBuf::from(value("--history-dir")?)),
            "--poll-ms" => {
                poll_ms = value("--poll-ms")?
                    .parse()
                    .map_err(|err| format!("--poll-ms: {err}"))?
            }
            "--report-secs" => {
                report_secs = value("--report-secs")?
                    .parse()
                    .map_err(|err| format!("--report-secs: {err}"))?
            }
            other => return Err(format!("unknown argument {other}\n\n{USAGE}")),
        }
    }
    Ok(Some(Args {
        ledger: ledger.ok_or(format!("--ledger is required\n\n{USAGE}"))?,
        listen_addr: SocketAddr::new(bind_address, port),
        allowed_hosts,
        history_dir,
        poll: Duration::from_millis(poll_ms.max(1)),
        report: Duration::from_secs(report_secs.max(1)),
    }))
}

/// True when SIGHUP was ignored at start, as under `nohup`.
fn hangup_ignored() -> bool {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("SigIgn:"))
                .and_then(|mask| u64::from_str_radix(mask.trim(), 16).ok())
        })
        .is_some_and(|mask| mask & 1 != 0)
}

/// Sets `stop` on SIGTERM, SIGINT or SIGHUP, so a closed terminal also drops the subscribers.
/// SIGHUP is left alone when it was ignored at start, so `nohup` keeps it running.
fn watch_signals(stop: Arc<AtomicBool>) -> std::io::Result<thread::JoinHandle<()>> {
    let watch_hangup = !hangup_ignored();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    thread::Builder::new()
        .name("evDashSig".to_string())
        .spawn(move || {
            runtime.block_on(async move {
                let (Ok(mut term), Ok(mut int)) = (
                    signal(SignalKind::terminate()),
                    signal(SignalKind::interrupt()),
                ) else {
                    log::error!("events: no signal handlers");
                    return;
                };
                let mut hup = watch_hangup
                    .then(|| signal(SignalKind::hangup()))
                    .transpose()
                    .unwrap_or_else(|err| {
                        log::error!("events: no SIGHUP handler: {err}");
                        None
                    });
                let hangup = async {
                    match hup.as_mut() {
                        Some(hup) => hup.recv().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    _ = term.recv() => log::info!("events: SIGTERM"),
                    _ = int.recv() => log::info!("events: SIGINT"),
                    _ = hangup => log::info!("events: SIGHUP"),
                }
                stop.store(true, Ordering::Relaxed);
            });
        })
}

fn main() -> ExitCode {
    agave_logger::setup_with_default("info");
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Some(args)) => args,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    let exit = Arc::new(AtomicBool::new(false));
    let start_progress = Arc::new(RwLock::new(ValidatorStartProgress::default()));
    let addr = args.listen_addr;
    let mut config = DashboardConfig::new(addr);
    // Added to the defaults rather than replacing them, so a proxied page stays reachable on the
    // box it runs on.
    config
        .allowed_hosts
        .extend(args.allowed_hosts.iter().cloned());
    config.history_dir = args.history_dir.clone();
    let mut service =
        match DashboardService::start(config, start_progress.clone(), exit.clone(), None) {
            Ok(service) => service,
            Err(err) => {
                eprintln!("dashboard on {addr}: {err}");
                return ExitCode::FAILURE;
            }
        };
    if let Err(err) = watch_signals(exit.clone()) {
        eprintln!("signals: {err}");
        return ExitCode::FAILURE;
    }
    log::info!(
        "dashboard on http://{addr}, reading {}",
        args.ledger.display()
    );

    let mirror = Arc::new(Mirror::new(start_progress.clone()));
    if let Some(dir) = &args.history_dir {
        mirror.keep_caught_up_in(dir);
    }
    service.report_startup_from(mirror.reported());
    let ledger = args.ledger.clone();
    let mut input = EventInput::new(ledger.join("events"), mirror.clone(), service.metrics_tap());
    let mut admin_read: Option<Instant> = None;
    let mut admin_pid = None;
    let mut attached = false;
    let mut last_report = Instant::now();
    let mut watch = ValidatorWatch::default();
    let mut restart = false;
    while !exit.load(Ordering::Relaxed) {
        let now = Instant::now();
        input.connect(now);
        input.drain();
        // Phases before the validator's event system exists come from its admin socket.
        if !mirror.progress_reported()
            && admin_read.is_none_or(|at| now.duration_since(at) >= ADMIN_POLL)
        {
            admin_read = Some(now);
            if let Some(progress) = admin_socket::start_progress(&ledger) {
                // First, so the first phase can be timed from the validator's start.
                if admin_pid.is_none() {
                    admin_pid = admin_socket::pid(&ledger);
                    // The validator's memory, threads and start time while it starts.
                    if let Some(pid) = admin_pid {
                        agave_dashboard::host_stats::read_process(pid);
                    }
                }
                *start_progress.write().unwrap() = progress;
                mirror.reported().write().unwrap().readings = true;
            }
        }
        if watch.gone(mirror.validator_pid().or(admin_pid), now) {
            log::info!("events: the validator exited or was replaced; starting again");
            restart = true;
            break;
        }
        // The collector starts once the mirror holds what it reads once; see `Mirror::ready`.
        if !attached && mirror.ready() {
            mirror.drop_backlog();
            match service.attach_source(mirror.clone()) {
                Ok(()) => {
                    attached = true;
                    log::info!("events: collector attached");
                }
                Err(err) => log::error!("events: attach: {err}"),
            }
        }
        if now.duration_since(last_report) >= args.report {
            last_report = now;
            log::info!("\n{}", input.report());
        }
        thread::sleep(args.poll);
    }

    log::info!("\n{}", input.report());
    input.disconnect();
    log::info!("events: subscribers dropped");
    if restart {
        exit.store(true, Ordering::Relaxed);
    }
    if service.join().is_err() {
        log::error!("events: a dashboard thread panicked");
    }
    if restart {
        let mut argv = std::env::args_os();
        let program = argv.next().unwrap_or_else(|| "agave-dashboard".into());
        let err = Command::new(program).args(argv).exec();
        log::error!("events: could not start again: {err}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// How often the validator's process is looked for.
const WATCH_EVERY: Duration = Duration::from_secs(1);
/// How often the admin socket is asked the start phase.
const ADMIN_POLL: Duration = Duration::from_millis(250);

/// Follows the validator's process id, from its node info, start progress or admin socket.
#[derive(Default)]
struct ValidatorWatch {
    pid: Option<u32>,
    /// Whether `/proc` showed the process, so a validator in another pid namespace is not taken
    /// for gone.
    visible: bool,
    checked: Option<Instant>,
}

impl ValidatorWatch {
    /// True once the watched validator's process has exited, or another validator's process id
    /// has arrived.
    fn gone(&mut self, pid: Option<u32>, now: Instant) -> bool {
        if self
            .checked
            .is_some_and(|at| now.duration_since(at) < WATCH_EVERY)
        {
            return false;
        }
        self.checked = Some(now);
        let Some(pid) = pid else {
            return false;
        };
        let alive = Path::new("/proc").join(pid.to_string()).exists();
        match self.pid {
            None => {
                self.pid = Some(pid);
                self.visible = alive;
                false
            }
            Some(held) if held != pid => true,
            Some(_) => self.visible && !alive,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(args: &[&str]) -> Result<Option<Args>, String> {
        parse_args(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn test_only_the_ledger_is_required() {
        let args = parsed(&["--ledger", "/ledger"]).unwrap().unwrap();
        assert_eq!(args.ledger, PathBuf::from("/ledger"));
        assert_eq!(
            args.listen_addr,
            SocketAddr::from((Ipv4Addr::LOCALHOST, DEFAULT_PORT))
        );
        assert!(args.allowed_hosts.is_empty());
        assert_eq!(args.history_dir, None);
        assert!(parsed(&[]).unwrap_err().starts_with("--ledger is required"));
    }

    #[test]
    fn test_each_flag_is_read() {
        let args = parsed(&[
            "--ledger",
            "/ledger",
            "--port",
            "3011",
            "--bind-address",
            "10.0.0.2",
            "--allowed-host",
            "a.example",
            "--allowed-host",
            "b.example",
            "--history-dir",
            "/history",
            "--poll-ms",
            "0",
            "--report-secs",
            "30",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(args.listen_addr, "10.0.0.2:3011".parse().unwrap());
        assert_eq!(args.allowed_hosts, ["a.example", "b.example"]);
        assert_eq!(args.history_dir, Some(PathBuf::from("/history")));
        assert_eq!(args.poll, Duration::from_millis(1));
        assert_eq!(args.report, Duration::from_secs(30));
    }

    #[test]
    fn test_old_and_unknown_flags_are_refused() {
        assert!(parsed(&["--ledger", "/l", "--events-dir", "/l/events"]).is_err());
        assert!(parsed(&["--ledger", "/l", "--leave-handles"]).is_err());
        assert!(parsed(&["--ledger"]).is_err());
        assert!(parsed(&["--ledger", "--port", "3011"]).is_err());
        assert!(parsed(&["--ledger", "/l", "--port", "ten"]).is_err());
        assert!(parsed(&["--ledger", "/l", "--bind-address", "::1"]).is_err());
        assert_eq!(parsed(&["--help"]), Ok(None));
    }

    #[test]
    fn test_a_value_can_follow_an_equals_sign() {
        let args = parsed(&["--ledger=/ledger", "--port=3011"])
            .unwrap()
            .unwrap();
        assert_eq!(args.ledger, PathBuf::from("/ledger"));
        assert_eq!(args.listen_addr.port(), 3011);
    }
}
