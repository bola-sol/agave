//! Publishes the node's state as event streams: banks, ledger, gossip, validator info and the
//! metrics datapoints, read through the validator's handles.

use {
    super::{
        bank_view::BankView,
        blocks::{Block, TxOutcome},
        metric_forward::MetricForward,
        metric_types::{
            METRIC_ACCOUNTS_STREAM, METRIC_GOSSIP_STREAM, METRIC_LEADER_STREAM, METRIC_NET_STREAM,
            METRIC_ONCE_STREAM, METRIC_SLOT_STREAM, METRIC_TPU_STREAM, METRIC_VOTOR_STREAM,
            MetricAccounts, MetricEvent, MetricGossip, MetricLeader, MetricNet, MetricOnce,
            MetricSlot, MetricTpu, MetricVotor,
        },
        reader::Reader,
        types::{
            BITMAP_BYTES, BLOCK_FOOTER, BLOCK_MISSING, BLOCK_OPAQUE, BLOCK_STREAM, BlockEvent,
            LEDGER_SLOT_STREAM, LedgerSlotEvent, MESSAGE_VERSIONS_STREAM, MessageVersionsEvent,
            OUTCOME_FAILED, OUTCOME_SUCCEEDED, OUTCOME_UNKNOWN, OWN_BLOCK_STREAM, OwnBlockEvent,
            OwnSetPlace, TX_OUTCOME_STREAM, TxOutcomeEvent,
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

/// Slots below the highest whose blockstore records are watched.
const LEDGER_WINDOW: u64 = 64;
/// How often our blocks this epoch go out whole, besides at turn-on and a new epoch or identity.
const OWN_EVERY: Duration = Duration::from_secs(10);
/// How often points sent once an hour, a run or an epoch go again, for a subscriber that missed them.
const LATCHED_EVERY: Duration = Duration::from_secs(60);
/// Metrics points held for the publisher.
const METRIC_QUEUE: usize = 16_384;
/// Set events sent before a pause, so a set larger than its stream arrives whole.
const PACE_BATCH: usize = 512;
const PACE_PAUSE: Duration = Duration::from_millis(50);
/// How long an unreadable block of ours near the root is read again each tick, as a leader's own
/// block can be rooted before its last shreds are stored.
const SETTLE_FOR: Duration = Duration::from_secs(5);
/// Slots under the root within which an unreadable block may still be settling.
const SETTLE_NEAR: u64 = 16;
/// Blocks read per tick behind the root.
const BLOCKS_PER_TICK: u64 = 64;

pub(crate) struct EventOutput {
    outcomes: Publisher<TxOutcomeEvent>,
    versions: Publisher<MessageVersionsEvent>,
    /// Frozen banks whose blocks are not yet full, held for their transactions' results.
    held: std::collections::BTreeMap<solana_clock::Slot, Arc<dyn BankView>>,
    /// Blocks already read, so a bank still in bank forks is not read again.
    blocks_read: std::collections::BTreeSet<solana_clock::Slot>,
    ledger: Publisher<LedgerSlotEvent>,
    /// What has been sent of each watched slot: its first shred, and that it filled.
    ledger_sent: std::collections::BTreeMap<solana_clock::Slot, (bool, bool)>,
    blocks: Publisher<BlockEvent>,
    /// The next rooted slot whose block goes out.
    next_block: Option<solana_clock::Slot>,
    blocks_settle: Settle,
    own_blocks: Publisher<OwnBlockEvent>,
    /// Our blocks this epoch read so far; `None` while the stream is off or the schedule unknown.
    own: Option<OwnBlocks>,
    own_generation: u64,
}

impl EventOutput {
    pub(crate) fn new(event_system: &EventSystem) -> io::Result<Self> {
        Ok(Self {
            outcomes: stream(event_system, TX_OUTCOME_STREAM, 65_536)?,
            versions: stream(event_system, MESSAGE_VERSIONS_STREAM, 256)?,
            held: std::collections::BTreeMap::new(),
            blocks_read: std::collections::BTreeSet::new(),
            ledger: stream(event_system, LEDGER_SLOT_STREAM, 1024)?,
            ledger_sent: std::collections::BTreeMap::new(),
            blocks: stream(event_system, BLOCK_STREAM, 512)?,
            next_block: None,
            blocks_settle: Settle::default(),
            own_blocks: stream(event_system, OWN_BLOCK_STREAM, 1024)?,
            own: None,
            own_generation: 0,
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
        let now = Instant::now();
        let root_bank = src.root_bank();
        self.send_ledger(src);
        self.send_blocks(src, root_bank.as_ref(), now);
        self.send_own_blocks(src, root_bank.as_ref(), now);
        self.send_blocks_read(src);
    }
}

impl EventOutput {
    /// Each watched slot once its first shred arrives, and again once it fills.
    fn send_ledger(&mut self, src: &Reader) {
        if !self.ledger.is_enabled() {
            self.ledger_sent.clear();
            return;
        }
        // Under the tip for the slots arriving, and under bank forks' highest for the slots replay
        // is completing, which trail the tip far while a node catches up.
        let replaying = src.highest_slot();
        let tip = replaying.max(src.cluster_tip().unwrap_or_default());
        let window = |top: solana_clock::Slot| top.saturating_sub(LEDGER_WINDOW)..=top;
        self.ledger_sent = self
            .ledger_sent
            .split_off(&replaying.saturating_sub(LEDGER_WINDOW));
        let slots: std::collections::BTreeSet<solana_clock::Slot> =
            window(replaying).chain(window(tip)).collect();
        for slot in slots {
            let (first_sent, full_sent) = self.ledger_sent.get(&slot).copied().unwrap_or_default();
            if full_sent {
                continue;
            }
            let Some(first_shred_ms) = src.first_shred_time(slot) else {
                continue;
            };
            let full = src.is_full(slot);
            if first_sent && !full {
                continue;
            }
            let parent = src.parent_of(slot);
            let (pid, tid, timestamp_ns) = header();
            let _ = self.ledger.publish(&LedgerSlotEvent {
                pid,
                tid,
                timestamp_ns,
                slot,
                has_parent: parent.is_some(),
                parent_slot: parent.unwrap_or_default(),
                first_shred_ms,
                full,
            });
            self.ledger_sent.insert(slot, (true, full));
        }
    }

    /// The gossip table whole, with each peer's advertised snapshots.
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

    /// Each rooted slot's block, from the root at turn-on.
    fn send_blocks(&mut self, src: &Reader, root_bank: &dyn BankView, now: Instant) {
        if !self.blocks.is_enabled() {
            self.next_block = None;
            return;
        }
        let root = src.commitment().root.max(root_bank.slot());
        let mut next = *self.next_block.get_or_insert(root);
        let last = root.min(next.saturating_add(BLOCKS_PER_TICK).saturating_sub(1));
        let me = src.identity();
        while next <= last {
            let block = src.block(next, root);
            if matches!(block, Block::Opaque)
                && src.slot_leader(next, root_bank) == Some(me)
                && self.blocks_settle.holds(next, root, &block, now)
            {
                break;
            }
            let _ = self.blocks.publish(&block_event(next, block));
            next = next.saturating_add(1);
        }
        self.next_block = Some(next);
    }

    /// Our blocks this epoch up to the root, whole once read: at turn-on, at a new epoch or
    /// identity, and every `OWN_EVERY`.
    fn send_own_blocks(&mut self, src: &Reader, root_bank: &dyn BankView, now: Instant) {
        if !self.own_blocks.is_enabled() {
            self.own = None;
            return;
        }
        let key = (root_bank.epoch(), src.identity());
        if self.own.as_ref().is_none_or(|own| own.key != key) {
            self.own = OwnBlocks::new(src, root_bank, key);
        }
        let Some(own) = self.own.as_mut() else {
            return;
        };
        let root = root_bank.slot();
        if !own.read_to(|slot| src.block(slot, root), root, src.lowest_slot(), now)
            || own
                .sent
                .is_some_and(|at| now.duration_since(at) < OWN_EVERY)
        {
            return;
        }
        self.own_generation = self.own_generation.saturating_add(1);
        own.sent = Some(now);
        // Past about 2,000 of our slots the pauses would fill a tick.
        publish_paced(
            &mut self.own_blocks,
            &own_block_events(key, self.own_generation, root, &own.events),
        );
    }
}

/// Our blocks of one epoch, read a tick's worth at a time.
struct OwnBlocks {
    key: (solana_clock::Epoch, solana_pubkey::Pubkey),
    leader_slots: Vec<solana_clock::Slot>,
    next: usize,
    events: Vec<BlockEvent>,
    sent: Option<Instant>,
    settle: Settle,
}

impl OwnBlocks {
    fn new(
        src: &Reader,
        bank: &dyn BankView,
        key: (solana_clock::Epoch, solana_pubkey::Pubkey),
    ) -> Option<Self> {
        let (epoch, me) = key;
        Some(Self::with_slots(
            key,
            src.leader_slots_in_epoch(bank, epoch, &me)?,
        ))
    }

    fn with_slots(
        key: (solana_clock::Epoch, solana_pubkey::Pubkey),
        leader_slots: Vec<solana_clock::Slot>,
    ) -> Self {
        Self {
            key,
            leader_slots,
            next: 0,
            events: Vec::new(),
            sent: None,
            settle: Settle::default(),
        }
    }

    #[cfg(test)]
    fn read_kinds(&self) -> Vec<(solana_clock::Slot, u8)> {
        self.events
            .iter()
            .map(|event| (event.slot, event.kind))
            .collect()
    }

    /// Reads up to `BLOCKS_PER_TICK` more of our slots up to `root`, from a ledger holding slots
    /// from `lowest`; whether all are read.
    fn read_to(
        &mut self,
        read_block: impl Fn(solana_clock::Slot) -> Block,
        root: solana_clock::Slot,
        lowest: solana_clock::Slot,
        now: Instant,
    ) -> bool {
        let mut read = 0u64;
        while let Some(slot) = self.leader_slots.get(self.next).copied() {
            if slot > root || read == BLOCKS_PER_TICK {
                break;
            }
            let block = read_block(slot);
            if self.settle.holds(slot, root, &block, now) {
                break;
            }
            self.events
                .push(block_event(slot, held_block(slot, lowest, block)));
            self.next = self.next.saturating_add(1);
            read = read.saturating_add(1);
        }
        self.leader_slots
            .get(self.next)
            .is_none_or(|slot| *slot > root)
    }
}

/// A slot under the ledger's first slot reads as skipped once cleaned; it is unreadable instead.
fn held_block(slot: solana_clock::Slot, lowest: solana_clock::Slot, block: Block) -> Block {
    match block {
        Block::Missing if slot < lowest => Block::Opaque,
        block => block,
    }
}

/// The first time a block read unreadable, so it is read again for `SETTLE_FOR`.
#[derive(Debug, Default)]
struct Settle(Option<(solana_clock::Slot, Instant)>);

impl Settle {
    /// Whether `block`, read up to `root` at `now`, is held back for another read: unreadable,
    /// near the root, and not yet held for `SETTLE_FOR`.
    fn holds(
        &mut self,
        slot: solana_clock::Slot,
        root: solana_clock::Slot,
        block: &Block,
        now: Instant,
    ) -> bool {
        matches!(block, Block::Opaque)
            && slot.saturating_add(SETTLE_NEAR) > root
            && self.hold(slot, now)
    }

    /// Whether `slot`, unreadable at `now`, is held back for another read.
    fn hold(&mut self, slot: solana_clock::Slot, now: Instant) -> bool {
        let since = match self.0 {
            Some((held, since)) if held == slot => since,
            _ => {
                self.0 = Some((slot, now));
                now
            }
        };
        now.duration_since(since) < SETTLE_FOR
    }
}

/// Our blocks as one set; with none, one event with `count` 0.
pub fn own_block_events(
    (epoch, identity): (solana_clock::Epoch, solana_pubkey::Pubkey),
    generation: u64,
    covered_to: solana_clock::Slot,
    blocks: &[BlockEvent],
) -> Vec<OwnBlockEvent> {
    let count = u32::try_from(blocks.len()).unwrap_or(u32::MAX);
    let (pid, tid, timestamp_ns) = header();
    let place = |index| OwnSetPlace {
        pid,
        tid,
        timestamp_ns,
        epoch,
        identity: identity.to_bytes(),
        generation,
        index,
        count,
        covered_to,
    };
    if blocks.is_empty() {
        return vec![OwnBlockEvent::new(place(0), &block_event(0, Block::Opaque))];
    }
    blocks
        .iter()
        .zip(0u32..)
        .map(|(block, index)| OwnBlockEvent::new(place(index), block))
        .collect()
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

fn publish_paced<E: Event>(publisher: &mut Publisher<E>, events: &[E]) {
    for (batch, events) in events.chunks(PACE_BATCH).enumerate() {
        if batch > 0 {
            thread::sleep(PACE_PAUSE);
        }
        for event in events {
            let _ = publisher.publish(event);
        }
    }
}

pub fn block_event(slot: solana_clock::Slot, block: Block) -> BlockEvent {
    let (pid, tid, timestamp_ns) = header();
    let mut event = BlockEvent {
        pid,
        tid,
        timestamp_ns,
        slot,
        kind: BLOCK_OPAQUE,
        close_nanos: 0,
        has_notar: false,
        notar_len: 0,
        notar: [0u8; BITMAP_BYTES],
        has_skip: false,
        skip_len: 0,
        skip: [0u8; BITMAP_BYTES],
        has_fast: false,
        fast: false,
    };
    let pack = |bitmap: &[u8], into: &mut [u8; BITMAP_BYTES]| -> Option<u16> {
        into.get_mut(..bitmap.len())?.copy_from_slice(bitmap);
        u16::try_from(bitmap.len()).ok()
    };
    match block {
        Block::Missing => event.kind = BLOCK_MISSING,
        Block::Pending | Block::Opaque => {}
        Block::Footer(footer) => {
            let notar = match &footer.notar_bitmap {
                Some(bitmap) => pack(bitmap, &mut event.notar).map(Some),
                None => Some(None),
            };
            let skip = match &footer.skip_bitmap {
                Some(bitmap) => pack(bitmap, &mut event.skip).map(Some),
                None => Some(None),
            };
            // A bitmap too long for the event leaves the block unread, as an unreadable one does.
            if let (Some(notar), Some(skip)) = (notar, skip) {
                event.kind = BLOCK_FOOTER;
                event.close_nanos = footer.close_nanos;
                event.has_notar = notar.is_some();
                event.notar_len = notar.unwrap_or(0);
                event.has_skip = skip.is_some();
                event.skip_len = skip.unwrap_or(0);
                event.has_fast = footer.fast.is_some();
                event.fast = footer.fast.unwrap_or(false);
            }
        }
    }
    event
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

#[cfg(test)]
mod tests {
    use {super::*, crate::node_events::blocks::FooterFacts, std::time::Duration};

    fn footer(close_nanos: u64) -> Block {
        Block::Footer(Box::new(FooterFacts {
            close_nanos,
            notar_bitmap: Some(vec![1, 2]),
            skip_bitmap: None,
            fast: Some(true),
        }))
    }

    /// Turns 1, 2 and 6 of a 32-slot epoch.
    fn our_slots() -> Vec<solana_clock::Slot> {
        (4..=11).chain(24..=27).collect()
    }

    #[test]
    fn test_an_unreadable_block_is_held_for_a_while_then_let_go() {
        let mut settle = Settle::default();
        let start = Instant::now();
        let at = |millis| start + Duration::from_millis(millis);
        assert!(settle.hold(7, at(0)));
        assert!(settle.hold(7, at(4_999)));
        assert!(!settle.hold(7, at(5_000)));
        // Another slot starts its own wait.
        assert!(settle.hold(8, at(5_000)));
        assert!(settle.hold(8, at(9_999)));
    }

    #[test]
    fn test_only_an_unreadable_block_near_the_root_is_held() {
        let now = Instant::now();
        let mut settle = Settle::default();
        assert!(settle.holds(24, 31, &Block::Opaque, now));
        let mut settle = Settle::default();
        assert!(!settle.holds(4, 31, &Block::Opaque, now));
        assert!(!settle.holds(24, 31, &Block::Missing, now));
        assert!(!settle.holds(24, 31, &footer(1), now));
    }

    #[test]
    fn test_a_cleaned_slot_is_unreadable_not_skipped() {
        assert!(matches!(held_block(4, 5, Block::Missing), Block::Opaque));
        assert!(matches!(held_block(5, 5, Block::Missing), Block::Missing));
        assert!(matches!(held_block(4, 5, footer(1)), Block::Footer(_)));
    }

    #[test]
    fn test_the_sender_reads_our_slots_up_to_the_root_and_resumes() {
        let mut own = OwnBlocks::with_slots((0, solana_pubkey::Pubkey::new_unique()), our_slots());
        let now = Instant::now();
        assert!(own.read_to(|_| Block::Missing, 10, 0, now));
        let read: Vec<_> = own.read_kinds().iter().map(|(slot, _)| *slot).collect();
        assert_eq!(read, (4..=10).collect::<Vec<_>>());
        assert!(own.read_to(|_| Block::Missing, 31, 0, now));
        let read: Vec<_> = own.read_kinds().iter().map(|(slot, _)| *slot).collect();
        assert_eq!(read, our_slots());
    }

    #[test]
    fn test_old_unreadable_slots_of_ours_are_read_in_one_go() {
        let mut own = OwnBlocks::with_slots((0, solana_pubkey::Pubkey::new_unique()), our_slots());
        // 4 to 7 read as skipped under the ledger's first slot, 8 to 11 unreadable, 24 to 27 held.
        let read = |slot| match slot {
            8..=11 => Block::Opaque,
            _ => Block::Missing,
        };
        assert!(own.read_to(read, 31, 8, Instant::now()));
        let expected: Vec<_> = (4..=11)
            .map(|slot| (slot, BLOCK_OPAQUE))
            .chain((24..=27).map(|slot| (slot, BLOCK_MISSING)))
            .collect();
        assert_eq!(own.read_kinds(), expected);
    }

    #[test]
    fn test_an_unreadable_block_of_ours_near_the_root_holds_the_read() {
        let mut own = OwnBlocks::with_slots((0, solana_pubkey::Pubkey::new_unique()), our_slots());
        let read = |slot| match slot {
            24 => Block::Opaque,
            _ => Block::Missing,
        };
        assert!(!own.read_to(read, 31, 0, Instant::now()));
        assert_eq!(own.read_kinds().last().map(|(slot, _)| *slot), Some(11));
    }
}
