//! Publishes the node's state as event streams: banks, ledger, gossip, validator info and the
//! metrics datapoints, read through the validator's handles.

use {
    super::{
        bank_view::BankView,
        blocks::TxOutcome,
        metric_forward::MetricForward,
        metric_types::{
            METRIC_ACCOUNTS_STREAM, METRIC_GOSSIP_STREAM, METRIC_LEADER_STREAM, METRIC_NET_STREAM,
            METRIC_ONCE_STREAM, METRIC_SLOT_STREAM, METRIC_TPU_STREAM, METRIC_VOTOR_STREAM,
            MetricAccounts, MetricEvent, MetricGossip, MetricLeader, MetricNet, MetricOnce,
            MetricSlot, MetricTpu, MetricVotor,
        },
        reader::Reader,
        types::{
            MESSAGE_VERSIONS_STREAM, MessageVersionsEvent, OUTCOME_FAILED, OUTCOME_SUCCEEDED,
            OUTCOME_UNKNOWN, TX_OUTCOME_STREAM, TxOutcomeEvent,
        },
    },
    agave_event_system::{
        Event, EventSystem, StreamConfig,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
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
};

const TICK: Duration = Duration::from_millis(200);

/// `publishers` publishers on a new stream.
fn publishers<E: Event>(
    event_system: &EventSystem,
    name: StreamName,
    capacity: usize,
    publishers: usize,
) -> io::Result<Vec<Publisher<E>>> {
    let factory = event_system
        .create_stream::<E>(
            name,
            StreamConfig {
                capacity,
                publisher_slots: publishers,
                subscriber_slots: 8,
            },
        )
        .map_err(io::Error::other)?;
    (0..publishers)
        .map(|_| {
            factory
                .try_create_publisher()
                .ok_or_else(|| io::Error::other("no publisher slot on a new stream"))
        })
        .collect()
}

fn stream<E: Event>(
    event_system: &EventSystem,
    name: StreamName,
    capacity: usize,
) -> io::Result<Publisher<E>> {
    publishers(event_system, name, capacity, 1)?
        .pop()
        .ok_or_else(|| io::Error::other("no publisher"))
}

/// How often points sent once an hour, a run or an epoch go again, for a subscriber that missed them.
const LATCHED_EVERY: Duration = Duration::from_secs(60);
/// Metrics points held for the publisher.
const METRIC_QUEUE: usize = 16_384;

pub(crate) struct EventOutput {
    outcomes: Publisher<TxOutcomeEvent>,
    versions: Publisher<MessageVersionsEvent>,
    /// Frozen banks whose blocks are not yet full, held for their transactions' results.
    held: std::collections::BTreeMap<solana_clock::Slot, Arc<dyn BankView>>,
    /// Blocks already read, so a bank still in bank forks is not read again.
    blocks_read: std::collections::BTreeSet<solana_clock::Slot>,
}

impl EventOutput {
    pub(crate) fn new(event_system: &EventSystem) -> io::Result<Self> {
        Ok(Self {
            outcomes: stream(event_system, TX_OUTCOME_STREAM, 65_536)?,
            versions: stream(event_system, MESSAGE_VERSIONS_STREAM, 256)?,
            held: std::collections::BTreeMap::new(),
            blocks_read: std::collections::BTreeSet::new(),
        })
    }

    /// Runs on its own thread until `exit`.
    pub(crate) fn spawn(
        mut self,
        src: Arc<Reader>,
        exit: Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name("solEvtNode".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    self.tick(&src);
                    thread::sleep(TICK);
                }
            })
    }

    fn tick(&mut self, held_src: &Arc<Reader>) {
        let src = held_src.as_ref();
        self.send_blocks_read(src);
    }
}

impl EventOutput {
    /// Each frozen block's transactions once it is full, and our blocks' message versions.
    fn send_blocks_read(&mut self, src: &Reader) {
        let outcomes_on = self.outcomes.is_enabled();
        let versions_on = self.versions.is_enabled();
        if !outcomes_on && !versions_on {
            self.held.clear();
            self.blocks_read.clear();
            return;
        }
        let banks = src.banks();
        for (slot, bank) in banks.frozen {
            if !self.blocks_read.contains(&slot) {
                self.held.entry(slot).or_insert(bank);
            }
        }
        // Bank forks drops a bank the root passes; a block not full by then is not read.
        let root = banks.root.slot();
        self.held = self.held.split_off(&root);
        self.blocks_read = self.blocks_read.split_off(&root);
        let me = src.identity();
        let full: Vec<solana_clock::Slot> = self
            .held
            .keys()
            .copied()
            .filter(|slot| src.is_full(*slot))
            .collect();
        for slot in full {
            let Some(bank) = self.held.remove(&slot) else {
                continue;
            };
            self.blocks_read.insert(slot);
            let ours = src.slot_leader(slot, bank.as_ref()) == Some(me);
            // Versions first: the subscriber reads a block as full once its outcomes arrive.
            if versions_on
                && ours
                && let Some(tally) = src.message_versions(slot)
            {
                let (pid, tid, timestamp_ns) = header();
                let _ = self.versions.publish(&MessageVersionsEvent {
                    pid,
                    tid,
                    timestamp_ns,
                    slot,
                    legacy: tally.legacy,
                    v0: tally.v0,
                    v1: tally.v1,
                });
            }
            if outcomes_on {
                let outcomes = src
                    .tx_outcomes(slot, bank.as_ref(), ours, &|_| true)
                    .unwrap_or_default();
                for event in outcome_events(slot, ours, &outcomes) {
                    let _ = self.outcomes.publish(&event);
                }
            }
        }
    }
}

/// A block's outcomes, or one empty event where it had none.
fn outcome_events(
    slot: solana_clock::Slot,
    ours: bool,
    outcomes: &[TxOutcome],
) -> Vec<TxOutcomeEvent> {
    let (pid, tid, timestamp_ns) = header();
    let count = u32::try_from(outcomes.len()).unwrap_or(u32::MAX);
    let event = |index: u32| TxOutcomeEvent {
        pid,
        tid,
        timestamp_ns,
        slot,
        index,
        count,
        ours,
        has_signature: false,
        signature: [0u8; 64],
        payer: [0u8; 32],
        outcome: OUTCOME_UNKNOWN,
        priority_fee: 0,
        base_fee: 0,
    };
    if outcomes.is_empty() {
        return vec![event(0)];
    }
    outcomes
        .iter()
        .zip(0u32..)
        .map(|(outcome, index)| TxOutcomeEvent {
            has_signature: outcome.signature.is_some(),
            signature: outcome
                .signature
                .map_or([0u8; 64], |signature| signature.into()),
            payer: outcome.payer.to_bytes(),
            outcome: match outcome.failed {
                Some(false) => OUTCOME_SUCCEEDED,
                Some(true) => OUTCOME_FAILED,
                None => OUTCOME_UNKNOWN,
            },
            priority_fee: outcome.priority,
            base_fee: outcome.base,
            ..event(index)
        })
        .collect()
}

fn header() -> (i32, i32, u64) {
    let (tid, pid) = cached_thread_and_process_id();
    (pid, tid, monotonic_timestamp_ns())
}

/// Publishes the datapoints the forwarder hands over, each on its row's stream.
pub(crate) struct MetricOutput {
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
    pub(crate) fn new(event_system: &EventSystem) -> io::Result<Self> {
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
    pub(crate) fn spawn(mut self, exit: Arc<AtomicBool>) -> io::Result<Option<JoinHandle<()>>> {
        // Bounded, so a stalled publisher drops points rather than holding memory.
        let (sender, points) = crossbeam_channel::bounded(METRIC_QUEUE);
        let tap = MetricForward::global().clone();
        if !tap.forward_to(sender) {
            log::warn!("node events: the metrics forwarder is already taken in this process");
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
