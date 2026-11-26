//! The metrics datapoints the dashboard reads, forwarded from the process's one datapoint observer
//! onto eight `metric.*` streams, with the points sent once latched and sent again for
//! subscribers that join late.

pub mod forward;
pub mod types;

use {
    agave_event_system::{EventSystem, publisher::Publisher},
    forward::MetricForward,
    solana_metrics::datapoint::DataPoint,
    std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
    types::{
        METRIC_ACCOUNTS_STREAM, METRIC_GOSSIP_STREAM, METRIC_LEADER_STREAM, METRIC_NET_STREAM,
        METRIC_ONCE_STREAM, METRIC_SLOT_STREAM, METRIC_TPU_STREAM, METRIC_VOTOR_STREAM,
        MetricAccounts, MetricEvent, MetricGossip, MetricLeader, MetricNet, MetricOnce, MetricSlot,
        MetricTpu, MetricVotor,
    },
};

const TICK: Duration = Duration::from_millis(200);
/// How often points sent once an hour, a run or an epoch go again, for a subscriber that missed them.
const LATCHED_EVERY: Duration = Duration::from_secs(60);
/// Metrics points held for the publisher.
const METRIC_QUEUE: usize = 16_384;

fn stream<E: agave_event_system::Event>(
    event_system: &EventSystem,
    name: agave_event_system::stream_name::StreamName,
    capacity: usize,
) -> io::Result<Publisher<E>> {
    crate::bank_state_events::publisher(event_system, name, capacity).map_err(io::Error::other)
}

/// Publishes the datapoints the forwarder hands over, each on its row's stream.
pub struct MetricOutput {
    slot: Publisher<MetricSlot>,
    leader: Publisher<MetricLeader>,
    votor: Publisher<MetricVotor>,
    tpu: Publisher<MetricTpu>,
    net: Publisher<MetricNet>,
    accounts: Publisher<MetricAccounts>,
    gossip: Publisher<MetricGossip>,
    once: Publisher<MetricOnce>,
}

impl MetricOutput {
    pub fn new(event_system: &EventSystem) -> io::Result<Self> {
        Ok(Self {
            slot: stream(event_system, METRIC_SLOT_STREAM, 4096)?,
            // Workers report every 20 ms each.
            leader: stream(event_system, METRIC_LEADER_STREAM, 8192)?,
            // Votor reports every vote account at once.
            votor: stream(event_system, METRIC_VOTOR_STREAM, 8192)?,
            tpu: stream(event_system, METRIC_TPU_STREAM, 1024)?,
            net: stream(event_system, METRIC_NET_STREAM, 1024)?,
            accounts: stream(event_system, METRIC_ACCOUNTS_STREAM, 2048)?,
            gossip: stream(event_system, METRIC_GOSSIP_STREAM, 256)?,
            once: stream(event_system, METRIC_ONCE_STREAM, 16)?,
        })
    }

    /// Runs on its own thread until `exit`, turning forwarding on while any stream is.
    pub fn spawn(mut self, exit: Arc<AtomicBool>) -> io::Result<Option<JoinHandle<()>>> {
        // Bounded, so a stalled publisher drops points rather than holding memory.
        let (sender, points) = crossbeam_channel::bounded(METRIC_QUEUE);
        let tap = MetricForward::global().clone();
        if !tap.forward_to(sender) {
            log::warn!("metric events: the metrics forwarder is already taken in this process");
            return Ok(None);
        }
        thread::Builder::new()
            .name("solEvtMetric".to_string())
            .spawn(move || {
                let mut checked = Instant::now();
                let mut latched_sent: Option<Instant> = None;
                tap.set_forwarding(self.enabled());
                while !exit.load(Ordering::Relaxed) {
                    match points.recv_timeout(TICK) {
                        Ok((point, observed_ms)) => self.publish(&point, observed_ms),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                    if checked.elapsed() >= TICK {
                        checked = Instant::now();
                        let enabled = self.enabled();
                        tap.set_forwarding(enabled);
                        if !enabled {
                            latched_sent = None;
                        } else if latched_sent.is_none_or(|at| at.elapsed() >= LATCHED_EVERY) {
                            latched_sent = Some(Instant::now());
                            for (point, observed_ms) in tap.latched_points() {
                                self.publish(&point, observed_ms);
                            }
                        }
                    }
                }
                tap.set_forwarding(false);
            })
            .map(Some)
    }

    fn enabled(&mut self) -> bool {
        // Every stream is asked, so each one's turn-on is seen.
        [
            self.slot.is_enabled(),
            self.leader.is_enabled(),
            self.votor.is_enabled(),
            self.tpu.is_enabled(),
            self.net.is_enabled(),
            self.accounts.is_enabled(),
            self.gossip.is_enabled(),
            self.once.is_enabled(),
        ]
        .contains(&true)
    }

    fn publish(&mut self, point: &DataPoint, observed_ms: u64) {
        if let Some(event) = MetricSlot::from_point(point, observed_ms) {
            let _ = self.slot.publish(&event);
        } else if let Some(event) = MetricLeader::from_point(point, observed_ms) {
            let _ = self.leader.publish(&event);
        } else if let Some(event) = MetricVotor::from_point(point, observed_ms) {
            let _ = self.votor.publish(&event);
        } else if let Some(event) = MetricTpu::from_point(point, observed_ms) {
            let _ = self.tpu.publish(&event);
        } else if let Some(event) = MetricNet::from_point(point, observed_ms) {
            let _ = self.net.publish(&event);
        } else if let Some(event) = MetricAccounts::from_point(point, observed_ms) {
            let _ = self.accounts.publish(&event);
        } else if let Some(event) = MetricGossip::from_point(point, observed_ms) {
            let _ = self.gossip.publish(&event);
        } else if let Some(event) = MetricOnce::from_point(point, observed_ms) {
            let _ = self.once.publish(&event);
        }
    }
}
