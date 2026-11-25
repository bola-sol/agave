//! Publishes what the dashboard reads of the node as event streams, from inside the validator, so
//! a dashboard in its own process can rebuild it. Test branch only: each stream is read through
//! the same [`Source`] the collector reads, so this tests the event shapes, not where Agave would
//! publish them.

use {
    crate::{
        events::{
            ACCOUNT_PATHS, ADDR_ALPENGLOW, ADDR_BYTES, ADDR_GOSSIP, ADDR_RPC,
            ADDR_SERVE_REPAIR_UDP, ADDR_TPU_FORWARDS_QUIC, ADDR_TPU_QUIC, ADDR_TPU_VOTE_QUIC,
            ADDR_TPU_VOTE_UDP, ADDR_TVU_UDP, ADDRS, ADMITTED_STREAM, AdmittedEvent, BITMAP_BYTES,
            BLOCK_FOOTER, BLOCK_MISSING, BLOCK_OPAQUE, BLOCK_STREAM, BlockEvent, EPOCH_STREAM,
            EpochEvent, FINALIZATION_RANKS, FINALIZATION_STREAM, FinalizationEvent,
            GOSSIP_PEER_STREAM, GossipPeerEvent, HISTORY_WORDS, ICON_BYTES, INFO_CHANGE,
            INFO_FULL_SET, LEDGER_SLOT_STREAM, LedgerSlotEvent, MESSAGE_VERSIONS_STREAM,
            MessageVersionsEvent, NAME_BYTES, NODE_INFO_STREAM, NodeInfoEvent, OUTCOME_FAILED,
            OUTCOME_SUCCEEDED, OUTCOME_UNKNOWN, PATH_BYTES, RANKS_PER_EVENT, ROLE_ROOT,
            ROLE_WORKING, SLOT_CLOCK_STREAM, SLOT_HISTORY_STREAM, SLOT_STATUS_STREAM,
            SlotClockEvent, SlotHistoryEvent, SlotStatusEvent, TX_OUTCOME_STREAM, TxOutcomeEvent,
            VALIDATOR_INFO_STREAM, VOTE_REWARD_BYTES, ValidatorInfoEvent, pack_addr, pack_path,
            pack_str,
        },
        metric_events::{
            METRIC_ACCOUNTS_STREAM, METRIC_GOSSIP_STREAM, METRIC_LEADER_STREAM, METRIC_NET_STREAM,
            METRIC_ONCE_STREAM, METRIC_SLOT_STREAM, METRIC_TPU_STREAM, METRIC_VOTOR_STREAM,
            MetricAccounts, MetricEvent, MetricGossip, MetricLeader, MetricNet, MetricOnce,
            MetricSlot, MetricTpu, MetricVotor,
        },
        metrics_tap::MetricsTap,
        source::{BankView, Source},
    },
    agave_event_system::{
        Event, EventSystem, StreamConfig,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
    agave_snapshots::SnapshotInterval,
    solana_metrics::datapoint::DataPoint,
    solana_vote_interface::state::VoteStateV4,
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
/// Unchanged events go out again this often, so a late subscriber has them soon.
const RESEND: Duration = Duration::from_secs(5);

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

/// What decides whether an event changed: everything but its header.
trait Keyed: Clone + PartialEq {
    fn unstamped(&self) -> Self;
}

macro_rules! keyed {
    ($($event:ty),*) => {$(
        impl Keyed for $event {
            fn unstamped(&self) -> Self {
                Self {
                    pid: 0,
                    tid: 0,
                    timestamp_ns: 0,
                    ..self.clone()
                }
            }
        }
    )*};
}

keyed!(NodeInfoEvent, SlotStatusEvent, EpochEvent, SlotClockEvent);

/// Sends an event when its content changes, when its stream turns on, and every [`RESEND`].
struct Resent<E: Event> {
    publisher: Publisher<E>,
    sent: Option<(E, Instant)>,
}

impl<E: Event + Keyed> Resent<E> {
    fn new(publisher: Publisher<E>) -> Self {
        Self {
            publisher,
            sent: None,
        }
    }

    fn enabled(&mut self) -> bool {
        let on = self.publisher.is_enabled();
        if !on {
            self.sent = None;
        }
        on
    }

    fn offer(&mut self, event: E, now: Instant) {
        let key = event.unstamped();
        let due = self
            .sent
            .as_ref()
            .is_none_or(|(held, at)| *held != key || now.duration_since(*at) >= RESEND);
        if due {
            let _ = self.publisher.publish(&event);
            self.sent = Some((key, now));
        }
    }
}

/// Slots below the highest whose blockstore records are watched.
const LEDGER_WINDOW: u64 = 64;
/// How often the rank maps go out whole, besides at turn-on and a new epoch.
const ADMITTED_EVERY: Duration = Duration::from_secs(5);
/// How often the slot history goes out whole; roots keep it current in between.
const HISTORY_EVERY: Duration = Duration::from_secs(5);
/// How often the gossip table goes out whole.
const PEERS_EVERY: Duration = Duration::from_secs(15);
/// How often every validator info entry goes out; changes go out as banks freeze.
const INFO_EVERY: Duration = Duration::from_secs(60);
/// How often points sent once an hour, a run or an epoch go again, for a subscriber that missed them.
const LATCHED_EVERY: Duration = Duration::from_secs(60);
/// Set events sent before a pause, so a set larger than its stream arrives whole.
const PACE_BATCH: usize = 512;
const PACE_PAUSE: Duration = Duration::from_millis(50);
/// Blocks read per tick behind the root.
const BLOCKS_PER_TICK: u64 = 64;

pub struct EventOutput {
    peers: Publisher<GossipPeerEvent>,
    peers_sent: Option<(u64, Instant)>,
    info: Publisher<ValidatorInfoEvent>,
    info_sent: Option<(u64, Instant)>,
    /// The names the in-process dashboard holds, sent whole every `INFO_EVERY`.
    info_cache: Arc<std::sync::RwLock<crate::validator_info::ValidatorInfoCache>>,
    /// When the in-process collector saw the node catch up, unix milliseconds, 0 before.
    caught_up: Arc<std::sync::atomic::AtomicU64>,
    finalization: Publisher<FinalizationEvent>,
    /// Frozen banks already read for finalization and validator info changes; `None` while both
    /// streams are off.
    frozen_seen: Option<std::collections::BTreeSet<solana_clock::Slot>>,
    outcomes: Publisher<TxOutcomeEvent>,
    versions: Publisher<MessageVersionsEvent>,
    /// Frozen banks whose blocks are not yet full, held for their transactions' results.
    held: std::collections::BTreeMap<solana_clock::Slot, Arc<dyn BankView>>,
    /// Blocks already read, so a bank still in bank forks is not read again.
    blocks_read: std::collections::BTreeSet<solana_clock::Slot>,
    admitted: Publisher<AdmittedEvent>,
    /// The epoch, generation and time of the last whole rank maps.
    admitted_sent: Option<(solana_clock::Epoch, u64, Instant)>,
    history: Publisher<SlotHistoryEvent>,
    history_sent: Option<(u64, Instant)>,
    ledger: Publisher<LedgerSlotEvent>,
    /// What has been sent of each watched slot: its first shred, and that it filled.
    ledger_sent: std::collections::BTreeMap<solana_clock::Slot, (bool, bool)>,
    blocks: Publisher<BlockEvent>,
    /// The next rooted slot whose block goes out.
    next_block: Option<solana_clock::Slot>,
    node: Resent<NodeInfoEvent>,
    status: Resent<SlotStatusEvent>,
    epoch: Resent<EpochEvent>,
    root: Resent<SlotClockEvent>,
    working: Resent<SlotClockEvent>,
}

impl EventOutput {
    pub fn new(
        event_system: &EventSystem,
        info_cache: Arc<std::sync::RwLock<crate::validator_info::ValidatorInfoCache>>,
        caught_up: Arc<std::sync::atomic::AtomicU64>,
    ) -> io::Result<Self> {
        let mut clock = publishers::<SlotClockEvent>(event_system, SLOT_CLOCK_STREAM, 256, 2)?;
        let (Some(working), Some(root)) = (clock.pop(), clock.pop()) else {
            return Err(io::Error::other("no publishers"));
        };
        Ok(Self {
            peers: stream(event_system, GOSSIP_PEER_STREAM, 16_384)?,
            peers_sent: None,
            info: stream(event_system, VALIDATOR_INFO_STREAM, 8192)?,
            info_sent: None,
            info_cache,
            caught_up,
            finalization: stream(event_system, FINALIZATION_STREAM, 1024)?,
            frozen_seen: None,
            outcomes: stream(event_system, TX_OUTCOME_STREAM, 65_536)?,
            versions: stream(event_system, MESSAGE_VERSIONS_STREAM, 256)?,
            held: std::collections::BTreeMap::new(),
            blocks_read: std::collections::BTreeSet::new(),
            admitted: stream(event_system, ADMITTED_STREAM, 1024)?,
            admitted_sent: None,
            history: stream(event_system, SLOT_HISTORY_STREAM, 256)?,
            history_sent: None,
            ledger: stream(event_system, LEDGER_SLOT_STREAM, 1024)?,
            ledger_sent: std::collections::BTreeMap::new(),
            blocks: stream(event_system, BLOCK_STREAM, 512)?,
            next_block: None,
            node: Resent::new(stream(event_system, NODE_INFO_STREAM, 16)?),
            status: Resent::new(stream(event_system, SLOT_STATUS_STREAM, 256)?),
            epoch: Resent::new(stream(event_system, EPOCH_STREAM, 16)?),
            working: Resent::new(working),
            root: Resent::new(root),
        })
    }

    /// Runs on its own thread until `exit`.
    pub fn spawn(
        mut self,
        src: Arc<dyn Source>,
        exit: Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name("solEvtDash".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    self.tick(&src);
                    thread::sleep(TICK);
                }
            })
    }

    fn tick(&mut self, held_src: &Arc<dyn Source>) {
        let src = held_src.as_ref();
        let now = Instant::now();
        self.send_ledger(src);
        self.send_blocks(src);
        self.send_admitted(src, now);
        self.send_history(src, now);
        self.send_blocks_read(src);
        self.send_peers(src, now);
        self.send_info(now);
        self.send_frozen_reads(src);
        if self.node.enabled() {
            self.node
                .offer(node_event(src, self.caught_up.load(Ordering::Relaxed)), now);
        }
        let wants_banks = [
            self.status.enabled(),
            self.epoch.enabled(),
            self.root.enabled(),
            self.working.enabled(),
        ];
        if !wants_banks.contains(&true) {
            return;
        }
        if wants_banks[0] {
            self.status.offer(status_event(src), now);
        }
        let working = src.working_bank();
        let identity = src.identity();
        let vote_account = src.vote_account();
        if wants_banks[1] {
            self.epoch
                .offer(epoch_event(working.as_ref(), &vote_account), now);
        }
        if wants_banks[3] {
            self.working.offer(
                clock_event(ROLE_WORKING, working.as_ref(), &identity, &vote_account),
                now,
            );
        }
        if wants_banks[2] {
            let root = src.root_bank();
            self.root.offer(
                clock_event(ROLE_ROOT, root.as_ref(), &identity, &vote_account),
                now,
            );
        }
    }
}

impl EventOutput {
    /// Each watched slot once its first shred arrives, and again once it fills.
    fn send_ledger(&mut self, src: &dyn Source) {
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
    fn send_peers(&mut self, src: &dyn Source, now: Instant) {
        if !self.peers.is_enabled() {
            self.peers_sent = None;
            return;
        }
        let generation = match self.peers_sent {
            Some((_, at)) if now.duration_since(at) < PEERS_EVERY => return,
            Some((generation, _)) => generation.saturating_add(1),
            None => 1,
        };
        self.peers_sent = Some((generation, now));
        let peers = src.all_peers();
        let keys: Vec<solana_pubkey::Pubkey> = peers.iter().map(|(peer, _)| peer.pubkey).collect();
        let snapshots = src.snapshot_slots(&keys);
        let (pid, tid, timestamp_ns) = header();
        let count = u32::try_from(peers.len()).unwrap_or(u32::MAX);
        let events: Vec<GossipPeerEvent> = peers
            .iter()
            .zip(snapshots)
            .zip(0u32..)
            .map(
                |(((peer, heard_ms), (full, incremental)), index)| GossipPeerEvent {
                    pid,
                    tid,
                    timestamp_ns,
                    generation,
                    index,
                    count,
                    pubkey: peer.pubkey.to_bytes(),
                    client: pack_str(&peer.client),
                    version: pack_str(&peer.version),
                    wallclock: peer.wallclock,
                    outset: peer.outset,
                    shred_version: peer.shred_version,
                    addrs: pack_addrs(peer),
                    heard_ms: *heard_ms,
                    has_full_snapshot: full.is_some(),
                    full_snapshot: full.unwrap_or(0),
                    has_incremental_snapshot: incremental.is_some(),
                    incremental_snapshot: incremental.unwrap_or(0),
                },
            )
            .collect();
        publish_paced(&mut self.peers, &events);
    }

    /// Every name the in-process dashboard holds, which its own scan at attach filled and frozen
    /// banks keep current, so a subscriber that joins late waits at most `INFO_EVERY`.
    fn send_info(&mut self, now: Instant) {
        if !self.info.is_enabled() {
            self.info_sent = None;
            return;
        }
        let generation = match self.info_sent {
            Some((_, at)) if now.duration_since(at) < INFO_EVERY => return,
            Some((generation, _)) => generation.saturating_add(1),
            None => 1,
        };
        let (slot, entries) = self.info_cache.read().unwrap().entries();
        // Tried again next tick until the dashboard's scan has read some.
        if entries.is_empty() {
            return;
        }
        let events = info_events(INFO_FULL_SET, generation, slot, &entries);
        publish_paced(&mut self.info, &events);
        self.info_sent = Some((generation, now));
    }

    /// Each new frozen bank's finalization reading and the validator info entries it changed.
    fn send_frozen_reads(&mut self, src: &dyn Source) {
        let finalization_on = self.finalization.is_enabled();
        let info_on = self.info.is_enabled();
        if !finalization_on && !info_on {
            self.frozen_seen = None;
            return;
        }
        let banks = src.banks();
        // At turn-on the banks already frozen are taken as read.
        let seen = self
            .frozen_seen
            .get_or_insert_with(|| banks.frozen.iter().map(|(slot, _)| *slot).collect());
        let fresh: Vec<_> = banks
            .frozen
            .iter()
            .filter(|(slot, _)| seen.insert(*slot))
            .collect();
        *seen = seen.split_off(&banks.root.slot());
        for (slot, bank) in fresh {
            if finalization_on && let Some(event) = finalization_event(bank.as_ref()) {
                let _ = self.finalization.publish(&event);
            }
            if info_on {
                let changed = bank.validator_info_changed();
                if !changed.is_empty() {
                    let generation = self.info_sent.map_or(0, |(generation, _)| generation);
                    for event in info_events(INFO_CHANGE, generation, *slot, &changed) {
                        let _ = self.info.publish(&event);
                    }
                }
            }
        }
    }

    /// Each frozen block's transactions once it is full, and our blocks' message versions.
    fn send_blocks_read(&mut self, src: &dyn Source) {
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

    /// The previous, this and the next epoch's rank maps from the working bank.
    fn send_admitted(&mut self, src: &dyn Source, now: Instant) {
        if !self.admitted.is_enabled() {
            self.admitted_sent = None;
            return;
        }
        let bank = src.working_bank();
        let epoch = bank.epoch();
        let generation = match self.admitted_sent {
            Some((sent, _, at)) if sent == epoch && now.duration_since(at) < ADMITTED_EVERY => {
                return;
            }
            Some((_, generation, _)) => generation.saturating_add(1),
            None => 1,
        };
        self.admitted_sent = Some((epoch, generation, now));
        let events: Vec<AdmittedEvent> = [epoch.saturating_sub(1), epoch, epoch.saturating_add(1)]
            .into_iter()
            .filter_map(|epoch| Some((epoch, bank.epoch_stakes(epoch)?)))
            .flat_map(|(epoch, stakes)| admitted_events(epoch, generation, &stakes))
            .collect();
        publish_paced(&mut self.admitted, &events);
    }

    /// The root bank's slot history, whole.
    fn send_history(&mut self, src: &dyn Source, now: Instant) {
        if !self.history.is_enabled() {
            self.history_sent = None;
            return;
        }
        let generation = match self.history_sent {
            Some((_, at)) if now.duration_since(at) < HISTORY_EVERY => return,
            Some((generation, _)) => generation.saturating_add(1),
            None => 1,
        };
        let Some(history) = src.root_bank().get_slot_history() else {
            return;
        };
        self.history_sent = Some((generation, now));
        publish_paced(&mut self.history, &history_events(&history, generation));
    }

    /// Each rooted slot's block, from the root at turn-on.
    fn send_blocks(&mut self, src: &dyn Source) {
        if !self.blocks.is_enabled() {
            self.next_block = None;
            return;
        }
        let root = src.commitment().root.max(src.root_bank().slot());
        let next = *self.next_block.get_or_insert(root);
        let last = root.min(next.saturating_add(BLOCKS_PER_TICK).saturating_sub(1));
        for slot in next..=last {
            let _ = self
                .blocks
                .publish(&block_event(slot, src.block(slot, root)));
        }
        if last >= next {
            self.next_block = Some(last.saturating_add(1));
        }
    }
}

fn pack_addrs(peer: &crate::source::PeerRow) -> [u8; ADDRS * ADDR_BYTES] {
    let mut addrs = [0u8; ADDRS * ADDR_BYTES];
    for (at, addr) in [
        (ADDR_GOSSIP, peer.gossip),
        (ADDR_RPC, peer.rpc),
        (ADDR_TVU_UDP, peer.tvu_udp),
        (ADDR_TPU_QUIC, peer.tpu_quic),
        (ADDR_TPU_FORWARDS_QUIC, peer.tpu_forwards_quic),
        (ADDR_TPU_VOTE_UDP, peer.tpu_vote_udp),
        (ADDR_TPU_VOTE_QUIC, peer.tpu_vote_quic),
        (ADDR_SERVE_REPAIR_UDP, peer.serve_repair_udp),
        (ADDR_ALPENGLOW, peer.alpenglow),
    ] {
        let start = at.saturating_mul(ADDR_BYTES);
        if let Some(slot) = addrs.get_mut(start..start.saturating_add(ADDR_BYTES)) {
            slot.copy_from_slice(&pack_addr(addr));
        }
    }
    addrs
}

pub(crate) fn info_events(
    reason: u8,
    generation: u64,
    slot: solana_clock::Slot,
    entries: &[(solana_pubkey::Pubkey, crate::validator_info::ValidatorInfo)],
) -> Vec<ValidatorInfoEvent> {
    let (pid, tid, timestamp_ns) = header();
    let count = u32::try_from(entries.len()).unwrap_or(u32::MAX);
    entries
        .iter()
        .zip(0u32..)
        .map(|((identity, info), index)| ValidatorInfoEvent {
            pid,
            tid,
            timestamp_ns,
            reason,
            generation,
            index,
            count,
            slot,
            identity: identity.to_bytes(),
            has_name: info.name.is_some(),
            name: pack_str::<NAME_BYTES>(info.name.as_deref().unwrap_or_default()),
            has_icon: info.icon_url.is_some(),
            icon: pack_str::<ICON_BYTES>(info.icon_url.as_deref().unwrap_or_default()),
        })
        .collect()
}

/// What the finalization share reads of a frozen bank: its ranked accounts' last votes against
/// the newest.
pub(crate) fn finalization_event(bank: &dyn BankView) -> Option<FinalizationEvent> {
    if !bank.is_alpenglow() {
        return None;
    }
    let ranks = bank.rank_map(bank.slot())?;
    let mut votes: Vec<(usize, solana_clock::Slot)> = Vec::new();
    bank.for_each_vote_row(&mut |row| {
        if let (Some(rank), Some(last)) = (ranks.rank_of(&row.vote), row.last_voted_slot) {
            votes.push((usize::from(rank), last));
        }
    });
    let front = votes.iter().map(|(_, slot)| *slot).max()?;
    let (pid, tid, timestamp_ns) = header();
    let mut event = FinalizationEvent {
        pid,
        tid,
        timestamp_ns,
        slot: bank.slot(),
        front,
        ranks: u16::try_from(ranks.len().min(FINALIZATION_RANKS)).unwrap_or(0),
        seen: [0u8; FINALIZATION_RANKS / 8],
        carried: [0u8; FINALIZATION_RANKS / 8],
    };
    for (rank, slot) in votes {
        let (byte, bit) = (rank / 8, rank % 8);
        let mask = 1u8
            .checked_shl(u32::try_from(bit).unwrap_or(0))
            .unwrap_or(0);
        if let Some(seen) = event.seen.get_mut(byte) {
            *seen |= mask;
        }
        if front.saturating_sub(slot) <= crate::collect::NEAR_SLOTS
            && let Some(carried) = event.carried.get_mut(byte)
        {
            *carried |= mask;
        }
    }
    Some(event)
}

/// A block's outcomes, or one empty event where it had none.
fn outcome_events(
    slot: solana_clock::Slot,
    ours: bool,
    outcomes: &[crate::collect::TxOutcome],
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

/// An epoch's rank map in parts of `RANKS_PER_EVENT`.
pub(crate) fn admitted_events(
    epoch: solana_clock::Epoch,
    generation: u64,
    stakes: &crate::source::EpochStakeView,
) -> Vec<AdmittedEvent> {
    let (pid, tid, timestamp_ns) = header();
    let ranks = stakes.ranks.len();
    let count = u32::try_from(ranks.div_ceil(RANKS_PER_EVENT)).unwrap_or(u32::MAX);
    (0..ranks)
        .step_by(RANKS_PER_EVENT)
        .zip(0u32..)
        .map(|(first, index)| {
            let mut event = AdmittedEvent {
                pid,
                tid,
                timestamp_ns,
                epoch,
                generation,
                index,
                count,
                total_stake: stakes.total_stake,
                first_rank: u32::try_from(first).unwrap_or(u32::MAX),
                len: 0,
                votes: [0u8; RANKS_PER_EVENT * 32],
                identities: [0u8; RANKS_PER_EVENT * 32],
                stakes: [0u64; RANKS_PER_EVENT],
            };
            let entries = (first..ranks.min(first.saturating_add(RANKS_PER_EVENT)))
                .filter_map(|rank| stakes.ranks.entry(rank));
            for (at, entry) in entries.enumerate() {
                let start = at.saturating_mul(32);
                if let Some(slot) = event.votes.get_mut(start..start.saturating_add(32)) {
                    slot.copy_from_slice(&entry.vote.to_bytes());
                }
                if let Some(slot) = event.identities.get_mut(start..start.saturating_add(32)) {
                    slot.copy_from_slice(&entry.identity.to_bytes());
                }
                if let Some(slot) = event.stakes.get_mut(at) {
                    *slot = entry.stake;
                }
                event.len = event.len.saturating_add(1);
            }
            event
        })
        .collect()
}

/// The slot history's bitmap in parts of `HISTORY_WORDS`.
pub(crate) fn history_events(
    history: &solana_slot_history::SlotHistory,
    generation: u64,
) -> Vec<SlotHistoryEvent> {
    use bv::Bits;
    let (pid, tid, timestamp_ns) = header();
    let words = history.bits.block_len();
    let count = u32::try_from(words.div_ceil(HISTORY_WORDS)).unwrap_or(u32::MAX);
    (0..words)
        .step_by(HISTORY_WORDS)
        .zip(0u32..)
        .map(|(first, index)| {
            let last = words.min(first.saturating_add(HISTORY_WORDS));
            let mut event = SlotHistoryEvent {
                pid,
                tid,
                timestamp_ns,
                generation,
                index,
                count,
                next_slot: history.next_slot,
                first_word: u32::try_from(first).unwrap_or(u32::MAX),
                len: u16::try_from(last.saturating_sub(first)).unwrap_or(0),
                words: [0u64; HISTORY_WORDS],
            };
            for (at, word) in (first..last).enumerate() {
                if let Some(slot) = event.words.get_mut(at) {
                    *slot = history.bits.get_block(word);
                }
            }
            event
        })
        .collect()
}

fn block_event(slot: solana_clock::Slot, block: crate::certs::Block) -> BlockEvent {
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
        crate::certs::Block::Missing => event.kind = BLOCK_MISSING,
        crate::certs::Block::Pending | crate::certs::Block::Opaque => {}
        crate::certs::Block::Footer(footer) => {
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

pub(crate) fn node_event(src: &dyn Source, caught_up_ms: u64) -> NodeInfoEvent {
    let (pid, tid, timestamp_ns) = header();
    let contact = src.my_contact();
    let mut addrs = [0u8; ADDRS * ADDR_BYTES];
    for (at, addr) in [
        (ADDR_GOSSIP, contact.gossip),
        (ADDR_RPC, contact.rpc),
        (ADDR_TVU_UDP, contact.tvu_udp),
        (ADDR_TPU_QUIC, contact.tpu_quic),
        (ADDR_TPU_FORWARDS_QUIC, contact.tpu_forwards_quic),
        (ADDR_TPU_VOTE_UDP, contact.tpu_vote_udp),
        (ADDR_TPU_VOTE_QUIC, contact.tpu_vote_quic),
        (ADDR_SERVE_REPAIR_UDP, contact.serve_repair_udp),
        (ADDR_ALPENGLOW, contact.alpenglow),
    ] {
        let start = at.saturating_mul(ADDR_BYTES);
        addrs[start..start.saturating_add(ADDR_BYTES)].copy_from_slice(&pack_addr(addr));
    }
    let mut account_paths = [0u8; ACCOUNT_PATHS * PATH_BYTES];
    for (packed, path) in account_paths
        .as_chunks_mut::<PATH_BYTES>()
        .0
        .iter_mut()
        .zip(src.account_paths())
    {
        packed.copy_from_slice(&pack_path(&path));
    }
    let snapshots = src.snapshot_config();
    let slots = |interval: SnapshotInterval| match interval {
        SnapshotInterval::Disabled => 0,
        SnapshotInterval::Slots(slots) => slots.get(),
    };
    NodeInfoEvent {
        pid,
        tid,
        timestamp_ns,
        identity: src.identity().to_bytes(),
        vote_account: src.vote_account().to_bytes(),
        cluster: match src.cluster_name() {
            "testnet" => 0,
            "mainnet" => 1,
            "devnet" => 2,
            _ => 3,
        },
        shred_version: src.shred_version(),
        version: pack_str(&contact.version),
        client: pack_str(&contact.client),
        wallclock: 0,
        outset: contact.outset,
        addrs,
        ledger_path: pack_path(&src.ledger_path()),
        account_paths,
        has_snapshots: snapshots.is_some(),
        snapshots_generated: snapshots
            .as_ref()
            .is_some_and(|config| config.should_generate_snapshots()),
        full_snapshot_dir: snapshots.as_ref().map_or([0u8; PATH_BYTES], |config| {
            pack_path(&config.full_snapshot_archives_dir)
        }),
        incremental_snapshot_dir: snapshots.as_ref().map_or([0u8; PATH_BYTES], |config| {
            pack_path(&config.incremental_snapshot_archives_dir)
        }),
        full_snapshot_interval: snapshots
            .as_ref()
            .map_or(0, |config| slots(config.full_snapshot_archive_interval)),
        incremental_snapshot_interval: snapshots.as_ref().map_or(0, |config| {
            slots(config.incremental_snapshot_archive_interval)
        }),
        global_addresses_only: src.global_addresses_only(),
        caught_up_ms,
    }
}

pub(crate) fn status_event(src: &dyn Source) -> SlotStatusEvent {
    let (pid, tid, timestamp_ns) = header();
    let commitment = src.commitment();
    let tip = src.cluster_tip();
    SlotStatusEvent {
        pid,
        tid,
        timestamp_ns,
        root: commitment.root,
        confirmed: commitment.confirmed,
        finalized: commitment.finalized,
        highest_slot: src.highest_slot(),
        has_cluster_tip: tip.is_some(),
        cluster_tip: tip.unwrap_or(0),
        lowest_slot: src.lowest_slot(),
    }
}

pub(crate) fn epoch_event(bank: &dyn BankView, vote_account: &solana_pubkey::Pubkey) -> EpochEvent {
    let (pid, tid, timestamp_ns) = header();
    let schedule = bank.epoch_schedule();
    let cost = bank.cost();
    let ticket_short = bank.vat_ticket_short(vote_account);
    let vote_reward = bank
        .account_data(&crate::collect::VOTE_REWARD_ACCOUNT)
        .filter(|data| data.len() <= VOTE_REWARD_BYTES)
        .unwrap_or_default();
    EpochEvent {
        pid,
        tid,
        timestamp_ns,
        epoch: bank.epoch(),
        slots_per_epoch: schedule.slots_per_epoch,
        leader_schedule_slot_offset: schedule.leader_schedule_slot_offset,
        warmup: schedule.warmup,
        first_normal_epoch: schedule.first_normal_epoch,
        first_normal_slot: schedule.first_normal_slot,
        ns_per_slot: u64::try_from(bank.ns_per_slot_at_slot(bank.slot())).unwrap_or(u64::MAX),
        alpenglow: bank.is_alpenglow(),
        lamports_per_signature: bank.get_lamports_per_signature(),
        block_cost_limit: cost.block_limit,
        account_cost_limit: cost.account_limit,
        minimum_vat_balance: bank.minimum_vote_account_balance_for_vat(),
        vote_account_rent: bank.get_minimum_balance_for_rent_exemption(VoteStateV4::size_of()),
        has_ticket_short: ticket_short.is_some(),
        ticket_short: ticket_short.unwrap_or(0),
        vote_reward_len: u8::try_from(vote_reward.len()).unwrap_or(0),
        vote_reward: {
            let mut packed = [0u8; VOTE_REWARD_BYTES];
            if let Some(slot) = packed.get_mut(..vote_reward.len()) {
                slot.copy_from_slice(&vote_reward);
            }
            packed
        },
    }
}

pub(crate) fn clock_event(
    role: u8,
    bank: &dyn BankView,
    identity: &solana_pubkey::Pubkey,
    vote_account: &solana_pubkey::Pubkey,
) -> SlotClockEvent {
    let (pid, tid, timestamp_ns) = header();
    let clock = bank.clock();
    let cost = bank.cost();
    let fees = bank.fees();
    SlotClockEvent {
        pid,
        tid,
        timestamp_ns,
        role,
        slot: bank.slot(),
        parent_slot: bank.parent_slot(),
        epoch: bank.epoch(),
        frozen: bank.is_frozen(),
        block_height: bank.block_height(),
        clock_slot: clock.slot,
        clock_epoch_start_timestamp: clock.epoch_start_timestamp,
        clock_epoch: clock.epoch,
        clock_leader_schedule_epoch: clock.leader_schedule_epoch,
        clock_unix_timestamp: clock.unix_timestamp,
        blockhash: bank.last_blockhash().to_bytes(),
        transaction_count: bank.transaction_count(),
        non_vote_transaction_count: bank.non_vote_transaction_count_since_restart(),
        transaction_error_count: bank.transaction_error_count(),
        entries_count: bank.transaction_entries_count(),
        block_cost: cost.block_cost,
        block_cost_limit: cost.block_limit,
        account_cost_limit: cost.account_limit,
        total_fees: fees.total,
        priority_fees: fees.priority,
        identity_balance: bank.get_balance(identity),
        vote_balance: bank.get_balance(vote_account),
    }
}

/// Publishes the datapoints the tap forwards, each on its row's stream.
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

    /// Runs on its own thread until `exit`, turning the tap's forwarding on while any stream is.
    pub fn spawn(
        mut self,
        tap: Arc<MetricsTap>,
        points: crossbeam_channel::Receiver<(DataPoint, u64)>,
        exit: Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>> {
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
