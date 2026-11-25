//! The dashboard page fed only from the validator's event streams, in its own process.
//! Test branch only.
//!
//! Usage: agave-dashboard-events --events-dir <ledger>/events [--port 3011] [--poll-ms 5]
//!        [--report-secs 10] [--history-dir <dir>] [--leave-handles]
//!
//! When the validator it reads exits or is replaced, it starts itself again from argv[0], since
//! the mirror and the collector hold the old validator's state.

use {
    agave_dashboard::{
        config::DashboardConfig, event_input::EventInput, mirror::Mirror, service::DashboardService,
    },
    solana_core::validator::ValidatorStartProgress,
    std::{
        net::{Ipv4Addr, SocketAddr},
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

struct Args {
    events_dir: PathBuf,
    port: u16,
    poll: Duration,
    report: Duration,
    leave_handles: bool,
    history_dir: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut events_dir = None;
    let mut port = 3011u16;
    let mut poll_ms = 5u64;
    let mut report_secs = 10u64;
    let mut leave_handles = false;
    let mut history_dir = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
        match arg.as_str() {
            "--events-dir" => events_dir = Some(PathBuf::from(value("--events-dir")?)),
            "--history-dir" => history_dir = Some(PathBuf::from(value("--history-dir")?)),
            "--port" => {
                port = value("--port")?
                    .parse()
                    .map_err(|err| format!("--port: {err}"))?
            }
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
            "--leave-handles" => leave_handles = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Args {
        events_dir: events_dir.ok_or("--events-dir is required")?,
        port,
        poll: Duration::from_millis(poll_ms.max(1)),
        report: Duration::from_secs(report_secs.max(1)),
        leave_handles,
        history_dir,
    })
}

/// Sets `stop` on SIGTERM, SIGINT or SIGHUP, so a closed terminal also drops the subscribers.
fn watch_signals(stop: Arc<AtomicBool>) -> std::io::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    thread::Builder::new()
        .name("evDashSig".to_string())
        .spawn(move || {
            runtime.block_on(async move {
                let (Ok(mut term), Ok(mut int), Ok(mut hup)) = (
                    signal(SignalKind::terminate()),
                    signal(SignalKind::interrupt()),
                    signal(SignalKind::hangup()),
                ) else {
                    log::error!("events: no signal handlers");
                    return;
                };
                tokio::select! {
                    _ = term.recv() => log::info!("events: SIGTERM"),
                    _ = int.recv() => log::info!("events: SIGINT"),
                    _ = hup.recv() => log::info!("events: SIGHUP"),
                }
                stop.store(true, Ordering::Relaxed);
            });
        })
}

fn main() -> ExitCode {
    agave_logger::setup_with_default("info");
    let args = match parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    let exit = Arc::new(AtomicBool::new(false));
    let start_progress = Arc::new(RwLock::new(ValidatorStartProgress::default()));
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let config = DashboardConfig {
        allowed_hosts: vec!["localhost".to_string(), "127.0.0.1".to_string()],
        history_dir: args.history_dir.clone(),
        ..DashboardConfig::new(addr)
    };
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
        "events dashboard on http://{addr}, reading {}",
        args.events_dir.display()
    );

    let mirror = Arc::new(Mirror::new(start_progress));
    let mut input = EventInput::new(args.events_dir, mirror.clone(), service.metrics_tap());
    let mut attached = false;
    let mut last_report = Instant::now();
    let mut watch = ValidatorWatch::default();
    let mut restart = false;
    while !exit.load(Ordering::Relaxed) {
        let now = Instant::now();
        input.connect(now);
        input.drain();
        if watch.gone(mirror.validator_pid(), now) {
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
    if args.leave_handles && !restart {
        log::info!("events: exiting without dropping the subscribers");
        std::process::exit(0);
    }
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
        let program = argv
            .next()
            .unwrap_or_else(|| "agave-dashboard-events".into());
        let err = Command::new(program).args(argv).exec();
        log::error!("events: could not start again: {err}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// How often the validator's process is looked for.
const WATCH_EVERY: Duration = Duration::from_secs(1);

/// Follows the process id the node info carries.
#[derive(Default)]
struct ValidatorWatch {
    pid: Option<u32>,
    /// Whether `/proc` showed the process, so a validator in another pid namespace is not taken
    /// for gone.
    visible: bool,
    checked: Option<Instant>,
}

impl ValidatorWatch {
    /// True once the watched validator's process has exited, or another validator's node info
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
