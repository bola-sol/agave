//! The dashboard page fed only from the validator's event streams, in its own process.
//! Test branch only.
//!
//! Usage: agave-dashboard-events --events-dir <ledger>/events [--port 3011] [--poll-ms 5]
//!        [--report-secs 10] [--leave-handles]

use {
    agave_dashboard::{
        collect::Replies,
        event_input::EventInput,
        history::{PACKED_SLOTS, SlotHistory},
        proto::{Publisher, TOPIC_SUMMARY},
        server::{self, Handles},
        validator_info::ValidatorInfoCache,
    },
    std::{
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
        process::ExitCode,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant, SystemTime},
    },
    tokio::{
        net::TcpListener,
        signal::unix::{SignalKind, signal},
    },
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

struct Args {
    events_dir: PathBuf,
    port: u16,
    poll: Duration,
    report: Duration,
    leave_handles: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut events_dir = None;
    let mut port = 3011u16;
    let mut poll_ms = 5u64;
    let mut report_secs = 10u64;
    let mut leave_handles = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
        match arg.as_str() {
            "--events-dir" => events_dir = Some(PathBuf::from(value("--events-dir")?)),
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

    let publisher = Arc::new(Publisher::new());
    publisher.publish(
        TOPIC_SUMMARY,
        "startup_time_nanos",
        &SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64),
    );
    let epochs = Arc::new(RwLock::new(Vec::new()));
    let handles = Handles {
        publisher: publisher.clone(),
        history: Arc::new(RwLock::new(SlotHistory::new(PACKED_SLOTS))),
        info_cache: Arc::new(RwLock::new(ValidatorInfoCache::default())),
        epochs: epochs.clone(),
        replies: Arc::new(Replies::default()),
        rewind: None,
    };

    let stop = Arc::new(AtomicBool::new(false));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("evDashRt")
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let listener = match runtime.block_on(TcpListener::bind(addr)) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("bind {addr}: {err}");
            return ExitCode::FAILURE;
        }
    };
    log::info!(
        "events dashboard on http://{addr}, reading {}",
        args.events_dir.display()
    );
    let allowed_hosts: Arc<[String]> =
        vec!["localhost".to_string(), "127.0.0.1".to_string()].into();
    runtime.spawn(server::serve(listener, handles, allowed_hosts));
    {
        let stop = stop.clone();
        runtime.spawn(async move {
            let (Ok(mut term), Ok(mut int)) = (
                signal(SignalKind::terminate()),
                signal(SignalKind::interrupt()),
            ) else {
                log::error!("events: no signal handlers");
                return;
            };
            tokio::select! {
                _ = term.recv() => log::info!("events: SIGTERM"),
                _ = int.recv() => log::info!("events: SIGINT"),
            }
            stop.store(true, Ordering::Relaxed);
        });
    }

    let mut input = EventInput::new(args.events_dir, epochs);
    let mut last_sample: Option<Instant> = None;
    let mut last_report = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        input.connect(now);
        input.drain(&publisher);
        if last_sample.is_none_or(|at| now.duration_since(at) >= SAMPLE_INTERVAL) {
            last_sample = Some(now);
            if let Some(sample) = input.throughput.sample(now, SystemTime::now()) {
                input.throughput.publish(sample, &publisher);
            }
        }
        if now.duration_since(last_report) >= args.report {
            last_report = now;
            log::info!("\n{}", input.report());
        }
        thread::sleep(args.poll);
    }

    log::info!("\n{}", input.report());
    if args.leave_handles {
        log::info!("events: exiting without dropping the subscribers");
        std::process::exit(0);
    }
    input.disconnect();
    log::info!("events: subscribers dropped");
    runtime.shutdown_timeout(Duration::from_secs(1));
    ExitCode::SUCCESS
}
