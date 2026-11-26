//! The node rebuilt from event streams, read through the same [`Source`] the in-process
//! collector reads. Test branch only. What no stream carries yet reads as empty.

use {
    crate::{
        certs::{Block, FooterFacts},
        collect::{EpochInfo, TxOutcome},
        context::StartProgress,
        events::{
            ACCOUNT_PATHS, ADDR_ALPENGLOW, ADDR_BYTES, ADDR_GOSSIP, ADDR_RPC,
            ADDR_SERVE_REPAIR_UDP, ADDR_TPU_FORWARDS_QUIC, ADDR_TPU_QUIC, ADDR_TPU_VOTE_QUIC,
            ADDR_TPU_VOTE_UDP, ADDR_TVU_UDP, AdmittedEvent, BITMAP_BYTES, BLOCK_FOOTER,
            BLOCK_MISSING, BlockEvent, EpochEvent, FinalizationEvent, GossipPeerEvent,
            INFO_FULL_SET, LedgerSlotEvent, MessageVersionsEvent, NodeInfoEvent, OUTCOME_FAILED,
            OUTCOME_SUCCEEDED, OwnBlockEvent, PATH_BYTES, ROLE_ROOT, SlotClockEvent,
            SlotHistoryEvent, SlotStatusEvent, TxOutcomeEvent, ValidatorInfoEvent, unpack_addr,
            unpack_path, unpack_str,
        },
        snapshot,
        source::{
            BankView, Banks, Commitment, CostFacts, EpochStakeView, FeeTotals, PeerRow, RankEntry,
            RankRows, Ranks, Source, VoteRow,
        },
        startup::{
            GossipStake, GossipValidator, PhaseTiming, ReportedTimings, SharedReported, phase_name,
        },
        validator_info::ValidatorInfo,
        versions::TxVersions,
    },
    agave_snapshots::{
        SnapshotInterval,
        snapshot_config::{SnapshotConfig, SnapshotUsage},
    },
    solana_clock::{Clock, Epoch, Slot},
    solana_core::{
        bank_state_events::VoteAccountEvent,
        replay_stage::events::BankFrozenEvent,
        start_events::{StartProgressEvent, SupermajorityWaitEvent},
        validator::ValidatorStartProgress,
    },
    solana_epoch_schedule::EpochSchedule,
    solana_hash::Hash,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_slot_history::SlotHistory,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        net::SocketAddr,
        num::NonZeroU64,
        path::PathBuf,
        str::FromStr,
        sync::{Arc, RwLock},
    },
};

/// Epoch schedules kept: the previous, this and the next.
const EPOCHS_KEPT: usize = 3;
/// Slots of blockstore records and roots kept below the newest.
const LEDGER_KEPT: u64 = 8192;
/// Blocks kept for the certificate walks.
const BLOCKS_KEPT: usize = 4096;
/// Frozen banks held for a collector that has not taken them yet.
const NOTIFIED_KEPT: usize = 1024;
/// Epochs of rank maps kept.
const STAKES_KEPT: usize = 4;
/// How long a frozen bank waits for its finalization reading before the collector takes it.
const FINALIZATION_WAIT: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(50)
} else {
    std::time::Duration::from_secs(1)
};
/// How long slots above the newest block sent at its root read as pending after it arrived.
const BLOCKS_STALE: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(500)
} else {
    std::time::Duration::from_secs(20)
};
/// How long our blocks read as pending while their set is awaited.
const OWN_SET_WAIT: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(500)
} else {
    std::time::Duration::from_secs(60)
};

/// Rank maps by epoch, shared by every bank built from one state.
type Stakes = Arc<BTreeMap<Epoch, EpochStakeView>>;

/// What every rebuilt bank answers from the state it was built from.
#[derive(Clone, Default)]
struct Shared {
    stakes: Stakes,
    history: Option<Arc<SlotHistory>>,
}

impl Shared {
    fn rank_map(&self, schedule: &EpochSchedule, slot: Slot) -> Option<Arc<dyn Ranks>> {
        self.stakes
            .get(&schedule.get_epoch(slot))
            .map(|stakes| stakes.ranks.clone())
    }
}

/// The streams whose events arrive as sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetStream {
    Wait,
    Peers,
    Info,
    History,
    Admitted,
    Outcomes,
    OwnBlocks,
}

/// A set's parts as they arrive, by index, for one generation.
struct PendingSet<E> {
    generation: u64,
    count: u32,
    parts: BTreeMap<u32, E>,
}

impl<E> PendingSet<E> {
    /// The whole set once its last part arrives.
    fn add(
        pending: &mut Option<Self>,
        generation: u64,
        index: u32,
        count: u32,
        part: E,
    ) -> Option<Vec<E>> {
        if pending
            .as_ref()
            .is_none_or(|held| held.generation != generation)
        {
            *pending = Some(Self {
                generation,
                count,
                parts: BTreeMap::new(),
            });
        }
        let held = pending.as_mut()?;
        held.parts.insert(index, part);
        if held.parts.len() != usize::try_from(held.count).ok()? {
            return None;
        }
        pending
            .take()
            .map(|held| held.parts.into_values().collect())
    }
}

/// The node as `node.info` describes it.
#[derive(Debug, Clone)]
struct Node {
    identity: Pubkey,
    vote_account: Pubkey,
    cluster: &'static str,
    shred_version: u16,
    contact: PeerRow,
    ledger_path: PathBuf,
    account_paths: Vec<PathBuf>,
    snapshot_config: Option<SnapshotConfig>,
    global_addresses_only: bool,
    caught_up_ms: u64,
}

/// A peer's addresses as `NodeInfoEvent::addrs` and `GossipPeerEvent::addrs` hold them.
fn addr_at(addrs: &[u8], at: usize) -> Option<SocketAddr> {
    let start = at.saturating_mul(ADDR_BYTES);
    addrs
        .get(start..start.saturating_add(ADDR_BYTES))
        .and_then(unpack_addr)
}

impl From<&NodeInfoEvent> for Node {
    fn from(event: &NodeInfoEvent) -> Self {
        let identity = Pubkey::new_from_array(event.identity);
        let addr = |at: usize| {
            let start = at.saturating_mul(ADDR_BYTES);
            event
                .addrs
                .get(start..start.saturating_add(ADDR_BYTES))
                .and_then(unpack_addr)
        };
        let interval = |slots: u64| {
            NonZeroU64::new(slots).map_or(SnapshotInterval::Disabled, SnapshotInterval::Slots)
        };
        let snapshot_config = event.has_snapshots.then(|| SnapshotConfig {
            usage: if event.snapshots_generated {
                SnapshotUsage::LoadAndGenerate
            } else {
                SnapshotUsage::LoadOnly
            },
            full_snapshot_archive_interval: interval(event.full_snapshot_interval),
            incremental_snapshot_archive_interval: interval(event.incremental_snapshot_interval),
            full_snapshot_archives_dir: unpack_path(&event.full_snapshot_dir).unwrap_or_default(),
            incremental_snapshot_archives_dir: unpack_path(&event.incremental_snapshot_dir)
                .unwrap_or_default(),
            ..SnapshotConfig::default()
        });
        Self {
            identity,
            vote_account: Pubkey::new_from_array(event.vote_account),
            cluster: match event.cluster {
                0 => "testnet",
                1 => "mainnet",
                2 => "devnet",
                _ => "development",
            },
            shred_version: event.shred_version,
            contact: PeerRow {
                pubkey: identity,
                client: unpack_str(&event.client),
                version: unpack_str(&event.version),
                wallclock: event.wallclock,
                outset: event.outset,
                shred_version: event.shred_version,
                gossip: addr(ADDR_GOSSIP),
                rpc: addr(ADDR_RPC),
                tvu_udp: addr(ADDR_TVU_UDP),
                tpu_quic: addr(ADDR_TPU_QUIC),
                tpu_forwards_quic: addr(ADDR_TPU_FORWARDS_QUIC),
                tpu_vote_udp: addr(ADDR_TPU_VOTE_UDP),
                tpu_vote_quic: addr(ADDR_TPU_VOTE_QUIC),
                serve_repair_udp: addr(ADDR_SERVE_REPAIR_UDP),
                alpenglow: addr(ADDR_ALPENGLOW),
            },
            ledger_path: unpack_path(&event.ledger_path).unwrap_or_default(),
            account_paths: event
                .account_paths
                .as_chunks::<PATH_BYTES>()
                .0
                .iter()
                .take(ACCOUNT_PATHS)
                .filter_map(|packed| unpack_path(packed))
                .collect(),
            snapshot_config,
            global_addresses_only: event.global_addresses_only,
            caught_up_ms: event.caught_up_ms,
        }
    }
}

fn peer_row(event: &GossipPeerEvent) -> PeerRow {
    let addr = |at: usize| addr_at(&event.addrs, at);
    PeerRow {
        pubkey: Pubkey::new_from_array(event.pubkey),
        client: unpack_str(&event.client),
        version: unpack_str(&event.version),
        wallclock: event.wallclock,
        outset: event.outset,
        shred_version: event.shred_version,
        gossip: addr(ADDR_GOSSIP),
        rpc: addr(ADDR_RPC),
        tvu_udp: addr(ADDR_TVU_UDP),
        tpu_quic: addr(ADDR_TPU_QUIC),
        tpu_forwards_quic: addr(ADDR_TPU_FORWARDS_QUIC),
        tpu_vote_udp: addr(ADDR_TPU_VOTE_UDP),
        tpu_vote_quic: addr(ADDR_TPU_VOTE_QUIC),
        serve_repair_udp: addr(ADDR_SERVE_REPAIR_UDP),
        alpenglow: addr(ADDR_ALPENGLOW),
    }
}

fn info_of(event: &ValidatorInfoEvent) -> (Pubkey, ValidatorInfo) {
    (
        Pubkey::new_from_array(event.identity),
        ValidatorInfo {
            name: event.has_name.then(|| unpack_str(&event.name)),
            icon_url: event.has_icon.then(|| unpack_str(&event.icon)),
        },
    )
}

/// What every bank of one epoch shares.
#[derive(Debug, Clone)]
struct EpochFacts {
    schedule: EpochSchedule,
    ns_per_slot: u64,
    alpenglow: bool,
    lamports_per_signature: u64,
    minimum_vat_balance: u64,
    vote_account_rent: u64,
    ticket_short: Option<u64>,
    vote_reward: Vec<u8>,
}

impl From<&EpochEvent> for EpochFacts {
    fn from(event: &EpochEvent) -> Self {
        Self {
            schedule: EpochSchedule {
                slots_per_epoch: event.slots_per_epoch,
                leader_schedule_slot_offset: event.leader_schedule_slot_offset,
                warmup: event.warmup,
                first_normal_epoch: event.first_normal_epoch,
                first_normal_slot: event.first_normal_slot,
            },
            ns_per_slot: event.ns_per_slot,
            alpenglow: event.alpenglow,
            lamports_per_signature: event.lamports_per_signature,
            minimum_vat_balance: event.minimum_vat_balance,
            vote_account_rent: event.vote_account_rent,
            ticket_short: event.has_ticket_short.then_some(event.ticket_short),
            vote_reward: event
                .vote_reward
                .get(..usize::from(event.vote_reward_len))
                .unwrap_or_default()
                .to_vec(),
        }
    }
}

/// A bank rebuilt from its slot clock event and the epoch's facts.
pub struct MirrorBank {
    clock: SlotClockEvent,
    epoch: Arc<EpochFacts>,
    identity: Pubkey,
    vote_account: Pubkey,
    votes: Arc<HashMap<Pubkey, VoteRow>>,
    shared: Shared,
}

impl BankView for MirrorBank {
    fn slot(&self) -> Slot {
        self.clock.slot
    }
    fn epoch(&self) -> Epoch {
        self.clock.epoch
    }
    fn epoch_schedule(&self) -> &EpochSchedule {
        &self.epoch.schedule
    }
    fn is_alpenglow(&self) -> bool {
        self.epoch.alpenglow
    }
    fn parent_slot(&self) -> Slot {
        self.clock.parent_slot
    }
    fn parent(&self) -> Option<Arc<dyn BankView>> {
        None
    }
    fn is_frozen(&self) -> bool {
        self.clock.frozen
    }
    fn block_height(&self) -> u64 {
        self.clock.block_height
    }
    fn clock(&self) -> Clock {
        Clock {
            slot: self.clock.clock_slot,
            epoch_start_timestamp: self.clock.clock_epoch_start_timestamp,
            epoch: self.clock.clock_epoch,
            leader_schedule_epoch: self.clock.clock_leader_schedule_epoch,
            unix_timestamp: self.clock.clock_unix_timestamp,
        }
    }
    fn ns_per_slot_at_slot(&self, _slot: Slot) -> u128 {
        u128::from(self.epoch.ns_per_slot)
    }
    fn last_blockhash(&self) -> Hash {
        Hash::new_from_array(self.clock.blockhash)
    }
    fn get_lamports_per_signature(&self) -> u64 {
        self.epoch.lamports_per_signature
    }
    fn transaction_count(&self) -> u64 {
        self.clock.transaction_count
    }
    fn non_vote_transaction_count_since_restart(&self) -> u64 {
        self.clock.non_vote_transaction_count
    }
    fn transaction_error_count(&self) -> u64 {
        self.clock.transaction_error_count
    }
    fn transaction_entries_count(&self) -> u64 {
        self.clock.entries_count
    }
    fn get_balance(&self, key: &Pubkey) -> u64 {
        if *key == self.identity {
            self.clock.identity_balance
        } else if *key == self.vote_account {
            self.clock.vote_balance
        } else {
            0
        }
    }
    fn cost(&self) -> CostFacts {
        CostFacts {
            block_cost: self.clock.block_cost,
            block_limit: self.clock.block_cost_limit,
            account_limit: self.clock.account_cost_limit,
        }
    }
    fn fees(&self) -> FeeTotals {
        FeeTotals {
            total: self.clock.total_fees,
            priority: self.clock.priority_fees,
        }
    }
    fn vote_row(&self, vote: &Pubkey) -> Option<VoteRow> {
        self.votes.get(vote).cloned()
    }
    fn for_each_vote_row(&self, each: &mut dyn FnMut(&VoteRow)) {
        for row in self.votes.values() {
            each(row);
        }
    }
    fn rank_map(&self, slot: Slot) -> Option<Arc<dyn Ranks>> {
        self.shared.rank_map(&self.epoch.schedule, slot)
    }
    fn epoch_stakes(&self, epoch: Epoch) -> Option<EpochStakeView> {
        self.shared.stakes.get(&epoch).cloned()
    }
    fn epoch_stakes_from_slot(&self, slot: Slot) -> Option<EpochStakeView> {
        self.epoch_stakes(self.epoch.schedule.get_epoch(slot))
    }
    fn vat_ticket_short(&self, vote: &Pubkey) -> Option<u64> {
        (*vote == self.vote_account)
            .then_some(self.epoch.ticket_short)
            .flatten()
    }
    fn minimum_vote_account_balance_for_vat(&self) -> u64 {
        self.epoch.minimum_vat_balance
    }
    fn get_minimum_balance_for_rent_exemption(&self, _len: usize) -> u64 {
        self.epoch.vote_account_rent
    }
    fn get_slot_history(&self) -> Option<SlotHistory> {
        self.shared.history.as_deref().cloned()
    }
    fn account_data(&self, key: &Pubkey) -> Option<Vec<u8>> {
        (*key == crate::collect::VOTE_REWARD_ACCOUNT && !self.epoch.vote_reward.is_empty())
            .then(|| self.epoch.vote_reward.clone())
    }
    fn validator_info_changed(&self) -> Vec<(Pubkey, ValidatorInfo)> {
        Vec::new()
    }
}

/// A frozen bank rebuilt from `replay.bank_frozen`.
#[derive(Clone)]
pub struct FrozenBank {
    event: BankFrozenEvent,
    epoch: Arc<EpochFacts>,
    votes: Arc<HashMap<Pubkey, VoteRow>>,
    shared: Shared,
    /// The parent as it froze, without its own parent.
    parent: Option<Arc<FrozenBank>>,
    /// What the finalization share reads of this bank, where it arrived.
    finalization: Option<Arc<FinalizationEvent>>,
    /// Whether readings were arriving when the collector took this bank, so one missing is lost
    /// rather than never sent.
    expects_finalization: bool,
    /// Validator info entries handed to the collector with this bank.
    info_changes: Vec<(Pubkey, ValidatorInfo)>,
}

impl BankView for FrozenBank {
    fn slot(&self) -> Slot {
        self.event.slot
    }
    fn epoch(&self) -> Epoch {
        self.epoch.schedule.get_epoch(self.event.slot)
    }
    fn epoch_schedule(&self) -> &EpochSchedule {
        &self.epoch.schedule
    }
    fn is_alpenglow(&self) -> bool {
        self.epoch.alpenglow
    }
    fn parent_slot(&self) -> Slot {
        self.event.parent_slot
    }
    fn parent(&self) -> Option<Arc<dyn BankView>> {
        self.parent
            .clone()
            .map(|parent| parent as Arc<dyn BankView>)
    }
    fn is_frozen(&self) -> bool {
        true
    }
    fn block_height(&self) -> u64 {
        self.event.block_height
    }
    fn clock(&self) -> Clock {
        Clock {
            slot: self.event.slot,
            ..Clock::default()
        }
    }
    fn ns_per_slot_at_slot(&self, _slot: Slot) -> u128 {
        u128::from(self.epoch.ns_per_slot)
    }
    fn last_blockhash(&self) -> Hash {
        Hash::new_from_array(self.event.blockhash)
    }
    fn get_lamports_per_signature(&self) -> u64 {
        self.epoch.lamports_per_signature
    }
    // The bank's count is the fork's running total; the event's own count is the block's.
    #[allow(clippy::misnamed_getters)]
    fn transaction_count(&self) -> u64 {
        self.event.transaction_count_total
    }
    fn non_vote_transaction_count_since_restart(&self) -> u64 {
        self.event.non_vote_transaction_count_total
    }
    fn transaction_error_count(&self) -> u64 {
        self.event.failed_transaction_count
    }
    fn transaction_entries_count(&self) -> u64 {
        self.event.transaction_entry_count
    }
    fn get_balance(&self, _key: &Pubkey) -> u64 {
        0
    }
    fn cost(&self) -> CostFacts {
        CostFacts {
            block_cost: self.event.block_cost,
            block_limit: self.event.block_cost_limit,
            account_limit: self.event.account_cost_limit,
        }
    }
    fn fees(&self) -> FeeTotals {
        FeeTotals {
            total: self.event.total_fee,
            priority: self.event.priority_fee,
        }
    }
    fn vote_row(&self, vote: &Pubkey) -> Option<VoteRow> {
        self.votes.get(vote).cloned()
    }
    /// From its finalization reading where it arrived: each ranked account's last vote set at
    /// the newest or just out of reach of it, which is all the share reads.
    fn for_each_vote_row(&self, each: &mut dyn FnMut(&VoteRow)) {
        let ranks = self.rank_map(self.event.slot);
        let (Some(reading), Some(ranks)) = (self.finalization.as_ref(), ranks) else {
            // Another bank's votes would count this block as carried by everyone.
            if !self.expects_finalization {
                for row in self.votes.values() {
                    each(row);
                }
            }
            return;
        };
        let bit = |bits: &[u8], rank: usize| {
            let shift = u32::try_from(rank % 8).unwrap_or(0);
            bits.get(rank / 8)
                .is_some_and(|byte| byte & 1u8.checked_shl(shift).unwrap_or(0) != 0)
        };
        let behind = reading
            .front
            .saturating_sub(crate::collect::NEAR_SLOTS.saturating_add(1));
        for rank in 0..usize::from(reading.ranks) {
            let Some(entry) = ranks.entry(rank) else {
                continue;
            };
            if !bit(&reading.seen, rank) {
                continue;
            }
            each(&VoteRow {
                vote: entry.vote,
                identity: entry.identity,
                stake: entry.stake,
                commission: 0,
                inflation_rewards_commission_bps: 0,
                last_voted_slot: Some(if bit(&reading.carried, rank) {
                    reading.front
                } else {
                    behind
                }),
                last_epoch_credits: None,
                pending_delegator_rewards: 0,
                has_bls_key: true,
            });
        }
    }
    fn rank_map(&self, slot: Slot) -> Option<Arc<dyn Ranks>> {
        self.shared.rank_map(&self.epoch.schedule, slot)
    }
    fn epoch_stakes(&self, epoch: Epoch) -> Option<EpochStakeView> {
        self.shared.stakes.get(&epoch).cloned()
    }
    fn epoch_stakes_from_slot(&self, slot: Slot) -> Option<EpochStakeView> {
        self.epoch_stakes(self.epoch.schedule.get_epoch(slot))
    }
    fn vat_ticket_short(&self, _vote: &Pubkey) -> Option<u64> {
        None
    }
    fn minimum_vote_account_balance_for_vat(&self) -> u64 {
        self.epoch.minimum_vat_balance
    }
    fn get_minimum_balance_for_rent_exemption(&self, _len: usize) -> u64 {
        self.epoch.vote_account_rent
    }
    fn get_slot_history(&self) -> Option<SlotHistory> {
        self.shared.history.as_deref().cloned()
    }
    fn account_data(&self, key: &Pubkey) -> Option<Vec<u8>> {
        (*key == crate::collect::VOTE_REWARD_ACCOUNT && !self.epoch.vote_reward.is_empty())
            .then(|| self.epoch.vote_reward.clone())
    }
    fn validator_info_changed(&self) -> Vec<(Pubkey, ValidatorInfo)> {
        self.info_changes.clone()
    }
}

/// A slot's blockstore record.
#[derive(Debug, Clone, Copy, Default)]
struct LedgerSlot {
    first_shred_ms: u64,
    parent: Option<Slot>,
    full: bool,
}

#[derive(Default)]
struct State {
    node: Option<Node>,
    status: Option<SlotStatusEvent>,
    epoch: Option<Arc<EpochFacts>>,
    root: Option<SlotClockEvent>,
    working: Option<SlotClockEvent>,
    votes: Arc<HashMap<Pubkey, VoteRow>>,
    /// Each epoch's turn leaders.
    schedules: BTreeMap<Epoch, Arc<Vec<Pubkey>>>,
    /// Frozen banks above the root.
    frozen: BTreeMap<Slot, Arc<FrozenBank>>,
    /// Frozen banks the collector has not taken yet, with when they arrived.
    notified: Vec<(Arc<FrozenBank>, std::time::Instant)>,
    finalization: BTreeMap<Slot, Arc<FinalizationEvent>>,
    /// Whether any finalization reading has arrived, so a bank waits for its own.
    finalization_seen: bool,
    peers: Vec<(PeerRow, u64)>,
    peer_at: HashMap<Pubkey, usize>,
    peer_snapshots: HashMap<Pubkey, (Option<Slot>, Option<Slot>)>,
    pending_peers: Option<PendingSet<GossipPeerEvent>>,
    /// Each validator's info, with the slot it was read at.
    info: HashMap<Pubkey, (Slot, ValidatorInfo)>,
    pending_info: Option<PendingSet<ValidatorInfoEvent>>,
    pending_wait: Option<PendingSet<SupermajorityWaitEvent>>,
    /// Entries not yet handed to the collector.
    info_changes: Vec<(Pubkey, ValidatorInfo)>,
    ledger: BTreeMap<Slot, LedgerSlot>,
    rooted: BTreeSet<Slot>,
    blocks: BTreeMap<Slot, BlockEvent>,
    /// The newest block sent at its root, and when it arrived.
    blocks_to: Option<(Slot, std::time::Instant)>,
    shared: Shared,
    /// Each full block's non-vote transactions, and whether it was ours.
    outcomes: BTreeMap<Slot, (bool, Vec<TxOutcome>)>,
    pending_outcomes: HashMap<Slot, Option<PendingSet<TxOutcomeEvent>>>,
    versions: BTreeMap<Slot, TxVersions>,
    pending_admitted: HashMap<Epoch, Option<PendingSet<AdmittedEvent>>>,
    pending_history: Option<PendingSet<SlotHistoryEvent>>,
    /// Our blocks this epoch from the newest whole set.
    own: Option<OwnSet>,
    pending_own: Option<PendingSet<OwnBlockEvent>>,
    /// Since when the own block stream has been connected; `None` while it is not.
    own_connected: Option<std::time::Instant>,
    /// The epoch and identity whose set our blocks wait for, and since when.
    own_awaited: Option<((Epoch, Pubkey), std::time::Instant)>,
}

/// Our blocks of one epoch, as the validator read them up to a root.
struct OwnSet {
    epoch: Epoch,
    identity: Pubkey,
    blocks: BTreeMap<Slot, BlockEvent>,
}

pub struct Mirror {
    state: RwLock<State>,
    start_progress: StartProgress,
    /// The publishing validator's process id, 0 until its node info or start progress arrives.
    validator_pid: std::sync::atomic::AtomicU32,
    reported: SharedReported,
}

impl Mirror {
    pub fn new(start_progress: StartProgress) -> Self {
        Self {
            state: RwLock::new(State::default()),
            start_progress,
            validator_pid: std::sync::atomic::AtomicU32::new(0),
            reported: SharedReported::default(),
        }
    }

    /// The validator's start as its events report it, for the startup publisher.
    pub fn reported(&self) -> SharedReported {
        self.reported.clone()
    }

    /// Whether a start progress event has arrived.
    pub fn progress_reported(&self) -> bool {
        self.reported.read().unwrap().timings.is_some()
    }

    fn note_pid(&self, pid: i32) {
        if let Ok(pid) = u32::try_from(pid) {
            crate::host_stats::read_process(pid);
            self.validator_pid
                .store(pid, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// The process id of the validator whose node info arrived last.
    pub fn validator_pid(&self) -> Option<u32> {
        let pid = self
            .validator_pid
            .load(std::sync::atomic::Ordering::Relaxed);
        (pid != 0).then_some(pid)
    }

    /// Whether enough has arrived for the collector to start. It reads some things once, as a
    /// running validator holds them from the start: the vote accounts, this epoch's schedule and
    /// rank map, and the slot history.
    pub fn ready(&self) -> bool {
        let state = self.state.read().unwrap();
        let (Some(epoch), Some(working)) = (state.epoch.as_ref(), state.working.as_ref()) else {
            return false;
        };
        let this_epoch = working.epoch;
        state.node.is_some()
            && state.status.is_some()
            && state.root.is_some()
            && !state.votes.is_empty()
            && state.schedules.contains_key(&this_epoch)
            && (!epoch.alpenglow || state.shared.stakes.contains_key(&this_epoch))
            && state.shared.history.is_some()
    }

    /// Drops one stream's partial sets after it resubscribes, since a new queue numbers
    /// generations afresh.
    pub fn reset_set(&self, stream: SetStream) {
        let mut state = self.state.write().unwrap();
        match stream {
            SetStream::Wait => state.pending_wait = None,
            SetStream::Peers => state.pending_peers = None,
            SetStream::Info => state.pending_info = None,
            SetStream::History => state.pending_history = None,
            SetStream::Admitted => state.pending_admitted.clear(),
            SetStream::Outcomes => state.pending_outcomes.clear(),
            SetStream::OwnBlocks => state.pending_own = None,
        }
    }

    /// Whether the own block stream is connected, and whether its queue is new.
    pub fn set_own_blocks_connected(&self, connected: bool, resubscribed: bool) {
        let mut state = self.state.write().unwrap();
        if !connected {
            state.own_connected = None;
        } else if resubscribed || state.own_connected.is_none() {
            state.own_connected = Some(std::time::Instant::now());
        }
    }

    /// Stops frozen banks waiting for finalization readings, as when that stream is off.
    pub fn reset_finalization(&self) {
        self.state.write().unwrap().finalization_seen = false;
    }

    /// Drops the banks frozen while the collector waited to start, which it never watched.
    pub fn drop_backlog(&self) {
        self.state.write().unwrap().notified.clear();
    }

    pub fn apply_node(&self, event: &NodeInfoEvent) {
        // The validator's memory and threads, not this process's.
        self.note_pid(event.pid);
        self.state.write().unwrap().node = Some(Node::from(event));
    }

    pub fn apply_progress(&self, event: &StartProgressEvent) {
        // The validator's memory and threads during start, before its node info.
        self.note_pid(event.pid);
        let progress = match event.phase {
            1 => ValidatorStartProgress::SearchingForRpcService,
            // The download ends before the event system exists.
            2 => ValidatorStartProgress::DownloadingSnapshot {
                slot: event.slot,
                rpc_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            },
            3 => ValidatorStartProgress::CleaningBlockStore,
            4 => ValidatorStartProgress::CleaningAccounts,
            5 => ValidatorStartProgress::LoadingLedger,
            6 => ValidatorStartProgress::ProcessingLedger {
                slot: event.slot,
                max_slot: event.max_slot,
            },
            7 => ValidatorStartProgress::StartingServices,
            8 => ValidatorStartProgress::Halted,
            9 => ValidatorStartProgress::WaitingForSupermajority {
                slot: event.slot,
                gossip_stake_percent: event.gossip_stake_percent,
            },
            10 => ValidatorStartProgress::Running,
            _ => ValidatorStartProgress::Initializing,
        };
        let len = usize::from(event.taken_len).min(event.taken_phases.len());
        let taken = event.taken_phases[..len]
            .iter()
            .zip(&event.taken_ns)
            .filter_map(|(phase, nanos)| {
                Some(PhaseTiming {
                    phase: phase_name(*phase)?.to_string(),
                    elapsed_nanos: *nanos,
                })
            })
            .collect();
        // The phase and its timings under one lock, so the publisher reads them as a pair.
        let mut reported = self.reported.write().unwrap();
        reported.readings = true;
        reported.progress = Some(progress);
        reported.timings = Some(ReportedTimings {
            phase: phase_name(event.phase).unwrap_or("initializing"),
            phase_elapsed_nanos: event.phase_elapsed_ns,
            at_ns: event.timestamp_ns,
            taken,
            replay_origin: event.has_replay_origin.then_some(event.replay_origin),
        });
        drop(reported);
        *self.start_progress.write().unwrap() = progress;
    }

    /// A whole check of the supermajority wait, with names where validator info has arrived.
    pub fn apply_wait(&self, event: &SupermajorityWaitEvent) {
        let mut state = self.state.write().unwrap();
        let Some(parts) = PendingSet::add(
            &mut state.pending_wait,
            event.generation,
            event.index,
            event.count,
            event.clone(),
        ) else {
            return;
        };
        let mut validators: Vec<GossipValidator> = parts
            .iter()
            .map(|part| {
                let identity = Pubkey::new_from_array(part.identity);
                GossipValidator {
                    identity: identity.to_string(),
                    name: state
                        .info
                        .get(&identity)
                        .and_then(|(_, info)| info.name.clone()),
                    version: part.seen.then(|| unpack_str(&part.version)),
                    stake: part.activated_stake,
                    seen: part.seen,
                }
            })
            .collect();
        drop(state);
        validators.sort_by(|a, b| {
            b.stake
                .cmp(&a.stake)
                .then_with(|| a.identity.cmp(&b.identity))
        });
        let seen = validators
            .iter()
            .filter(|validator| validator.seen)
            .fold(0u64, |sum, validator| sum.saturating_add(validator.stake));
        self.reported.write().unwrap().gossip = Some(GossipStake {
            slot: event.slot,
            shred_version: event.shred_version,
            total: event.total_activated_stake,
            seen,
            validators,
        });
    }

    pub fn apply_status(&self, event: &SlotStatusEvent) {
        let mut state = self.state.write().unwrap();
        // Bank forks keeps the root and drops the banks below it.
        state.frozen = state.frozen.split_off(&event.root);
        let floor = event.root.saturating_sub(LEDGER_KEPT);
        state.rooted = state.rooted.split_off(&floor);
        let highest = event.highest_slot.max(event.cluster_tip);
        state.ledger = state.ledger.split_off(&highest.saturating_sub(LEDGER_KEPT));
        state.status = Some(event.clone());
    }

    pub fn apply_frozen(&self, event: &BankFrozenEvent) {
        let mut state = self.state.write().unwrap();
        let epoch = state
            .epoch
            .clone()
            .unwrap_or_else(|| Arc::new(EpochFacts::from(&empty_epoch())));
        let parent = state.frozen.get(&event.parent_slot).map(|parent| {
            Arc::new(FrozenBank {
                parent: None,
                info_changes: Vec::new(),
                ..FrozenBank::clone(parent)
            })
        });
        let bank = Arc::new(FrozenBank {
            event: event.clone(),
            epoch,
            votes: state.votes.clone(),
            shared: state.shared.clone(),
            parent,
            finalization: None,
            expects_finalization: false,
            info_changes: Vec::new(),
        });
        state.frozen.insert(event.slot, bank.clone());
        state.notified.push((bank, std::time::Instant::now()));
        if state.notified.len() > NOTIFIED_KEPT {
            let excess = state.notified.len().saturating_sub(NOTIFIED_KEPT);
            state.notified.drain(..excess);
        }
    }

    pub fn apply_finalization(&self, event: &FinalizationEvent) {
        let mut state = self.state.write().unwrap();
        state.finalization_seen = true;
        state
            .finalization
            .insert(event.slot, Arc::new(event.clone()));
        while state.finalization.len() > NOTIFIED_KEPT {
            state.finalization.pop_first();
        }
    }

    pub fn apply_peer(&self, event: &GossipPeerEvent) {
        let mut state = self.state.write().unwrap();
        let Some(parts) = PendingSet::add(
            &mut state.pending_peers,
            event.generation,
            event.index,
            event.count,
            event.clone(),
        ) else {
            return;
        };
        state.peers = parts
            .iter()
            .map(|part| (peer_row(part), part.heard_ms))
            .collect();
        state.peer_at = state
            .peers
            .iter()
            .enumerate()
            .map(|(at, (peer, _))| (peer.pubkey, at))
            .collect();
        state.peer_snapshots = parts
            .iter()
            .map(|part| {
                (
                    Pubkey::new_from_array(part.pubkey),
                    (
                        part.has_full_snapshot.then_some(part.full_snapshot),
                        part.has_incremental_snapshot
                            .then_some(part.incremental_snapshot),
                    ),
                )
            })
            .collect();
    }

    pub fn apply_info(&self, event: &ValidatorInfoEvent) {
        let mut state = self.state.write().unwrap();
        if event.reason != INFO_FULL_SET {
            let (identity, info) = info_of(event);
            state.info.insert(identity, (event.slot, info.clone()));
            state.info_changes.push((identity, info));
            return;
        }
        let Some(parts) = PendingSet::add(
            &mut state.pending_info,
            event.generation,
            event.index,
            event.count,
            event.clone(),
        ) else {
            return;
        };
        for part in &parts {
            let (identity, info) = info_of(part);
            // A change read from a newer bank while the scan ran stays.
            if state
                .info
                .get(&identity)
                .is_some_and(|(slot, _)| *slot > part.slot)
            {
                continue;
            }
            if state.info.get(&identity).map(|(_, held)| held) != Some(&info) {
                state.info_changes.push((identity, info.clone()));
            }
            state.info.insert(identity, (part.slot, info));
        }
    }

    pub fn apply_rooted(&self, slot: Slot) {
        let mut state = self.state.write().unwrap();
        state.rooted.insert(slot);
        // A root already in the history, as one read after it, is left alone.
        if let Some(history) = state.shared.history.as_mut()
            && slot >= history.next_slot
        {
            Arc::make_mut(history).add(slot);
        }
    }

    pub fn apply_admitted(&self, event: &AdmittedEvent) {
        let mut state = self.state.write().unwrap();
        let pending = state.pending_admitted.entry(event.epoch).or_default();
        let Some(parts) = PendingSet::add(
            pending,
            event.generation,
            event.index,
            event.count,
            event.clone(),
        ) else {
            return;
        };
        let entries: Vec<RankEntry> = parts
            .iter()
            .flat_map(|part| {
                (0..usize::from(part.len)).filter_map(move |at| {
                    let key = |bytes: &[u8]| {
                        let start = at.saturating_mul(32);
                        let slice = bytes.get(start..start.saturating_add(32))?;
                        Some(Pubkey::new_from_array(slice.try_into().ok()?))
                    };
                    Some(RankEntry {
                        vote: key(&part.votes)?,
                        identity: key(&part.identities)?,
                        stake: *part.stakes.get(at)?,
                    })
                })
            })
            .collect();
        let total_stake = parts.first().map_or(0, |part| part.total_stake);
        let mut stakes = BTreeMap::clone(&state.shared.stakes);
        stakes.insert(
            event.epoch,
            EpochStakeView {
                total_stake,
                ranks: Arc::new(RankRows::new(entries)),
            },
        );
        while stakes.len() > STAKES_KEPT {
            stakes.pop_first();
        }
        state.shared.stakes = Arc::new(stakes);
        state.pending_admitted.remove(&event.epoch);
    }

    pub fn apply_history(&self, event: &SlotHistoryEvent) {
        use bv::BitsMut;
        let mut state = self.state.write().unwrap();
        let Some(parts) = PendingSet::add(
            &mut state.pending_history,
            event.generation,
            event.index,
            event.count,
            event.clone(),
        ) else {
            return;
        };
        let mut history = SlotHistory::default();
        for part in &parts {
            let first = usize::try_from(part.first_word).unwrap_or(usize::MAX);
            for (at, word) in part.words.iter().take(usize::from(part.len)).enumerate() {
                let position = first.saturating_add(at);
                if position < history.bits.block_len() {
                    history.bits.set_block(position, *word);
                }
            }
        }
        history.next_slot = parts.first().map_or(0, |part| part.next_slot);
        // Roots that arrived after the validator read the history, in order.
        for slot in state.rooted.range(history.next_slot..) {
            history.add(*slot);
        }
        state.shared.history = Some(Arc::new(history));
    }

    pub fn apply_ledger(&self, event: &LedgerSlotEvent) {
        self.state.write().unwrap().ledger.insert(
            event.slot,
            LedgerSlot {
                first_shred_ms: event.first_shred_ms,
                parent: event.has_parent.then_some(event.parent_slot),
                full: event.full,
            },
        );
    }

    pub fn apply_outcome(&self, event: &TxOutcomeEvent) {
        let mut state = self.state.write().unwrap();
        let pending = state.pending_outcomes.entry(event.slot).or_default();
        // An empty block's one event completes it.
        let count = event.count.max(1);
        let Some(parts) = PendingSet::add(pending, 0, event.index, count, event.clone()) else {
            return;
        };
        state.pending_outcomes.remove(&event.slot);
        let outcomes = parts
            .iter()
            .filter(|part| part.count > 0)
            .map(|part| TxOutcome {
                signature: part.has_signature.then(|| Signature::from(part.signature)),
                payer: Pubkey::new_from_array(part.payer),
                failed: match part.outcome {
                    OUTCOME_SUCCEEDED => Some(false),
                    OUTCOME_FAILED => Some(true),
                    _ => None,
                },
                priority: part.priority_fee,
                base: part.base_fee,
            })
            .collect();
        state.outcomes.insert(event.slot, (event.ours, outcomes));
        while state.outcomes.len() > BLOCKS_KEPT {
            state.outcomes.pop_first();
        }
        let floor = event.slot.saturating_sub(LEDGER_KEPT);
        state.pending_outcomes.retain(|slot, _| *slot >= floor);
    }

    pub fn apply_versions(&self, event: &MessageVersionsEvent) {
        let mut state = self.state.write().unwrap();
        state.versions.insert(
            event.slot,
            TxVersions {
                legacy: event.legacy,
                v0: event.v0,
                v1: event.v1,
            },
        );
        while state.versions.len() > BLOCKS_KEPT {
            state.versions.pop_first();
        }
    }

    pub fn apply_own_block(&self, event: &OwnBlockEvent) {
        let mut state = self.state.write().unwrap();
        let Some(parts) = PendingSet::add(
            &mut state.pending_own,
            event.generation,
            event.index,
            event.count.max(1),
            event.clone(),
        ) else {
            return;
        };
        state.own = Some(OwnSet {
            epoch: event.epoch,
            identity: Pubkey::new_from_array(event.identity),
            blocks: parts
                .iter()
                .filter(|part| part.count > 0)
                .map(|part| (part.slot, part.block()))
                .collect(),
        });
    }

    pub fn apply_block(&self, event: &BlockEvent) {
        let mut state = self.state.write().unwrap();
        let newest = state
            .blocks_to
            .map_or(event.slot, |(to, _)| to.max(event.slot));
        state.blocks_to = Some((newest, std::time::Instant::now()));
        state.blocks.insert(event.slot, event.clone());
        while state.blocks.len() > BLOCKS_KEPT {
            state.blocks.pop_first();
        }
    }

    pub fn apply_epoch(&self, event: &EpochEvent) {
        self.state.write().unwrap().epoch = Some(Arc::new(EpochFacts::from(event)));
    }

    pub fn apply_clock(&self, event: &SlotClockEvent) {
        let mut state = self.state.write().unwrap();
        if event.role == ROLE_ROOT {
            state.root = Some(event.clone());
        } else {
            state.working = Some(event.clone());
        }
    }

    /// A complete vote account set, or the set with changes applied.
    pub fn apply_votes<'a>(&self, accounts: impl Iterator<Item = &'a VoteAccountEvent>) {
        let rows = accounts
            .map(|event| {
                let vote = Pubkey::new_from_array(event.vote_account);
                let row = VoteRow {
                    vote,
                    identity: Pubkey::new_from_array(event.identity),
                    stake: event.activated_stake,
                    commission: event.commission,
                    inflation_rewards_commission_bps: event.inflation_rewards_commission_bps,
                    last_voted_slot: event.has_last_voted_slot.then_some(event.last_voted_slot),
                    last_epoch_credits: event.has_credits.then_some((
                        event.credits_epoch,
                        event.credits,
                        event.prev_credits,
                    )),
                    pending_delegator_rewards: event.pending_delegator_rewards,
                    has_bls_key: event.has_bls_key,
                };
                (vote, row)
            })
            .collect();
        self.state.write().unwrap().votes = Arc::new(rows);
    }

    /// A completed epoch schedule.
    pub fn apply_schedule(&self, info: &EpochInfo) {
        let Ok(keys) = info
            .leaders
            .iter()
            .map(|key| Pubkey::from_str(key))
            .collect::<Result<Vec<Pubkey>, _>>()
        else {
            return;
        };
        let Some(leaders) = info
            .turns
            .iter()
            .map(|index| keys.get(usize::from(*index)).copied())
            .collect::<Option<Vec<Pubkey>>>()
        else {
            return;
        };
        let mut state = self.state.write().unwrap();
        state.schedules.insert(info.epoch, Arc::new(leaders));
        while state.schedules.len() > EPOCHS_KEPT {
            state.schedules.pop_first();
        }
    }

    fn bank(&self, state: &State, clock: Option<&SlotClockEvent>) -> Arc<dyn BankView> {
        let node = state.node.as_ref();
        Arc::new(MirrorBank {
            clock: clock.cloned().unwrap_or_else(empty_clock),
            epoch: state
                .epoch
                .clone()
                .unwrap_or_else(|| Arc::new(EpochFacts::from(&empty_epoch()))),
            identity: node.map_or_else(Pubkey::default, |node| node.identity),
            vote_account: node.map_or_else(Pubkey::default, |node| node.vote_account),
            votes: state.votes.clone(),
            shared: state.shared.clone(),
        })
    }

    /// Bank forks' highest slot is never below a bank it holds, and frozen banks arrive ahead of
    /// the status that reports it.
    fn highest_of(state: &State) -> Slot {
        let reported = state
            .status
            .as_ref()
            .map_or(0, |status| status.highest_slot);
        let frozen = state.frozen.keys().next_back().copied().unwrap_or(0);
        let working = state.working.as_ref().map_or(0, |clock| clock.slot);
        reported.max(frozen).max(working)
    }

    /// Whether `slot` is ours in the root's epoch and that epoch's own block set is still awaited.
    fn own_awaited(&self, slot: Slot, root: Slot) -> bool {
        let state = self.state.read().unwrap();
        let (Some(connected), Some(facts), Some(me)) = (
            state.own_connected,
            state.epoch.as_ref(),
            state.node.as_ref().map(|node| node.identity),
        ) else {
            return false;
        };
        let schedule = &facts.schedule;
        let epoch = schedule.get_epoch(root);
        if schedule.get_epoch(slot) != epoch
            || Self::leader_of(&state, schedule, slot) != Some(me)
            || state
                .own
                .as_ref()
                .is_some_and(|own| own.epoch == epoch && own.identity == me)
        {
            return false;
        }
        let awaited = state.own_awaited;
        drop(state);
        let now = std::time::Instant::now();
        let since = match awaited {
            Some((key, since)) if key == (epoch, me) => since,
            _ => {
                self.state.write().unwrap().own_awaited = Some(((epoch, me), now));
                now
            }
        };
        now.duration_since(since.max(connected)) < OWN_SET_WAIT
    }

    /// The turn leader of `slot`, where its epoch's schedule has arrived.
    fn leader_of(state: &State, schedule: &EpochSchedule, slot: Slot) -> Option<Pubkey> {
        let (epoch, index) = schedule.get_epoch_and_slot_index(slot);
        let turn = index.checked_div(NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64)?;
        let turn = usize::try_from(turn).ok()?;
        state.schedules.get(&epoch)?.get(turn).copied()
    }
}

fn empty_clock() -> SlotClockEvent {
    SlotClockEvent {
        pid: 0,
        tid: 0,
        timestamp_ns: 0,
        role: 0,
        slot: 0,
        parent_slot: 0,
        epoch: 0,
        frozen: false,
        block_height: 0,
        clock_slot: 0,
        clock_epoch_start_timestamp: 0,
        clock_epoch: 0,
        clock_leader_schedule_epoch: 0,
        clock_unix_timestamp: 0,
        blockhash: [0u8; 32],
        transaction_count: 0,
        non_vote_transaction_count: 0,
        transaction_error_count: 0,
        entries_count: 0,
        block_cost: 0,
        block_cost_limit: 0,
        account_cost_limit: 0,
        total_fees: 0,
        priority_fees: 0,
        identity_balance: 0,
        vote_balance: 0,
    }
}

fn empty_epoch() -> EpochEvent {
    let schedule = EpochSchedule::default();
    EpochEvent {
        pid: 0,
        tid: 0,
        timestamp_ns: 0,
        epoch: 0,
        slots_per_epoch: schedule.slots_per_epoch,
        leader_schedule_slot_offset: schedule.leader_schedule_slot_offset,
        warmup: schedule.warmup,
        first_normal_epoch: schedule.first_normal_epoch,
        first_normal_slot: schedule.first_normal_slot,
        ns_per_slot: 400_000_000,
        alpenglow: false,
        lamports_per_signature: 0,
        block_cost_limit: 0,
        account_cost_limit: 0,
        minimum_vat_balance: 0,
        vote_account_rent: 0,
        has_ticket_short: false,
        ticket_short: 0,
        vote_reward_len: 0,
        vote_reward: [0u8; crate::events::VOTE_REWARD_BYTES],
    }
}

impl Source for Mirror {
    fn identity(&self) -> Pubkey {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .map_or_else(Pubkey::default, |node| node.identity)
    }
    fn vote_account(&self) -> Pubkey {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .map_or_else(Pubkey::default, |node| node.vote_account)
    }
    fn cluster_name(&self) -> &'static str {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .map_or("development", |node| node.cluster)
    }
    fn cluster_tip(&self) -> Option<Slot> {
        let state = self.state.read().unwrap();
        state
            .status
            .as_ref()
            .and_then(|status| status.has_cluster_tip.then_some(status.cluster_tip))
    }
    fn shred_version(&self) -> u16 {
        let state = self.state.read().unwrap();
        state.node.as_ref().map_or(0, |node| node.shred_version)
    }
    fn my_contact(&self) -> PeerRow {
        let state = self.state.read().unwrap();
        state.node.as_ref().map_or_else(
            || PeerRow {
                pubkey: Pubkey::default(),
                client: String::new(),
                version: String::new(),
                wallclock: 0,
                outset: 0,
                shred_version: 0,
                gossip: None,
                rpc: None,
                tvu_udp: None,
                tpu_quic: None,
                tpu_forwards_quic: None,
                tpu_vote_udp: None,
                tpu_vote_quic: None,
                serve_repair_udp: None,
                alpenglow: None,
            },
            |node| node.contact.clone(),
        )
    }
    fn all_peers(&self) -> Vec<(PeerRow, u64)> {
        self.state.read().unwrap().peers.clone()
    }
    fn peer_gossip(&self, key: &Pubkey) -> Option<SocketAddr> {
        let state = self.state.read().unwrap();
        if let Some(node) = state.node.as_ref().filter(|node| node.identity == *key) {
            return node.contact.gossip;
        }
        let at = *state.peer_at.get(key)?;
        state.peers.get(at).and_then(|(peer, _)| peer.gossip)
    }
    fn snapshot_slots(&self, keys: &[Pubkey]) -> Vec<(Option<Slot>, Option<Slot>)> {
        let state = self.state.read().unwrap();
        keys.iter()
            .map(|key| state.peer_snapshots.get(key).copied().unwrap_or_default())
            .collect()
    }
    fn rpc_node_count(&self, peers: &[(PeerRow, u64)]) -> usize {
        let me = self.identity();
        let space = if self.global_addresses_only() {
            solana_net_utils::SocketAddrSpace::Global
        } else {
            solana_net_utils::SocketAddrSpace::Unspecified
        };
        peers
            .iter()
            .filter(|(peer, _)| {
                peer.pubkey != me && peer.rpc.is_some_and(|addr| space.check(&addr))
            })
            .count()
    }
    fn reports_caught_up(&self) -> bool {
        true
    }
    fn caught_up_millis(&self) -> Option<u64> {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .map(|node| node.caught_up_ms)
            .filter(|millis| *millis > 0)
    }
    fn global_addresses_only(&self) -> bool {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .is_some_and(|node| node.global_addresses_only)
    }
    fn banks(&self) -> Banks {
        let state = self.state.read().unwrap();
        Banks {
            root: self.bank(&state, state.root.as_ref()),
            working: self.bank(&state, state.working.as_ref()),
            highest_slot: Self::highest_of(&state),
            frozen: state
                .frozen
                .iter()
                .map(|(slot, bank)| (*slot, bank.clone() as Arc<dyn BankView>))
                .collect(),
        }
    }
    fn root_bank(&self) -> Arc<dyn BankView> {
        let state = self.state.read().unwrap();
        self.bank(&state, state.root.as_ref())
    }
    fn working_bank(&self) -> Arc<dyn BankView> {
        let state = self.state.read().unwrap();
        self.bank(&state, state.working.as_ref())
    }
    fn highest_slot(&self) -> Slot {
        Self::highest_of(&self.state.read().unwrap())
    }
    /// A bank waits briefly for its finalization reading, which follows its freeze.
    fn take_frozen(&self) -> Option<Vec<Arc<dyn BankView>>> {
        let mut state = self.state.write().unwrap();
        let now = std::time::Instant::now();
        let waiting = state.finalization_seen;
        let (ready, held): (Vec<_>, Vec<_>) = std::mem::take(&mut state.notified)
            .into_iter()
            .partition(|(bank, arrived)| {
                !waiting
                    || state.finalization.contains_key(&bank.event.slot)
                    || now.duration_since(*arrived) >= FINALIZATION_WAIT
            });
        state.notified = held;
        let mut changes = std::mem::take(&mut state.info_changes);
        let banks = ready
            .into_iter()
            .map(|(bank, _)| {
                Arc::new(FrozenBank {
                    finalization: state.finalization.get(&bank.event.slot).cloned(),
                    expects_finalization: waiting,
                    info_changes: std::mem::take(&mut changes),
                    ..FrozenBank::clone(&bank)
                }) as Arc<dyn BankView>
            })
            .collect::<Vec<_>>();
        // With no bank to carry them, the changes wait for the next.
        state.info_changes = changes;
        Some(banks)
    }
    fn commitment(&self) -> Commitment {
        let state = self.state.read().unwrap();
        state
            .status
            .as_ref()
            .map_or_else(Commitment::default, |status| Commitment {
                root: status.root,
                confirmed: status.confirmed,
                finalized: status.finalized,
            })
    }
    fn first_shred_time(&self, slot: Slot) -> Option<u64> {
        let state = self.state.read().unwrap();
        state
            .ledger
            .get(&slot)
            .map(|ledger| ledger.first_shred_ms)
            .filter(|ms| *ms > 0)
    }
    fn parent_of(&self, slot: Slot) -> Option<Slot> {
        let state = self.state.read().unwrap();
        state.ledger.get(&slot).and_then(|ledger| ledger.parent)
    }
    fn is_root(&self, slot: Slot) -> bool {
        let state = self.state.read().unwrap();
        state.rooted.contains(&slot)
            || state.shared.history.as_ref().is_some_and(|history| {
                matches!(history.check(slot), solana_slot_history::Check::Found)
            })
    }
    /// A block reads as full once its transactions have arrived too, which follow its fill.
    fn is_full(&self, slot: Slot) -> bool {
        let state = self.state.read().unwrap();
        state.ledger.get(&slot).is_some_and(|ledger| ledger.full)
            && state.outcomes.contains_key(&slot)
    }
    fn lowest_slot(&self) -> Slot {
        let state = self.state.read().unwrap();
        state.status.as_ref().map_or(0, |status| status.lowest_slot)
    }
    fn ledger_path(&self) -> PathBuf {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .map_or_else(PathBuf::new, |node| node.ledger_path.clone())
    }
    fn message_versions(&self, slot: Slot) -> Option<TxVersions> {
        self.state.read().unwrap().versions.get(&slot).copied()
    }
    fn tx_outcomes(
        &self,
        slot: Slot,
        _bank: &dyn BankView,
        ours: bool,
        followed: &dyn Fn(&Pubkey) -> bool,
    ) -> Option<Vec<TxOutcome>> {
        let state = self.state.read().unwrap();
        let (_, outcomes) = state.outcomes.get(&slot)?;
        // Another leader's block counts only the payers followed, as in process.
        Some(
            outcomes
                .iter()
                .filter(|outcome| ours || followed(&outcome.payer))
                .copied()
                .collect(),
        )
    }
    fn block(&self, slot: Slot, root: Slot) -> Block {
        if slot > root {
            return Block::Pending;
        }
        let state = self.state.read().unwrap();
        let held = state
            .blocks
            .get(&slot)
            .or_else(|| state.own.as_ref().and_then(|own| own.blocks.get(&slot)));
        let Some(event) = held else {
            // The validator holds a block back while it settles, so its root can run ahead.
            let behind = state
                .blocks_to
                .is_some_and(|(to, at)| slot > to && at.elapsed() < BLOCKS_STALE);
            drop(state);
            return if behind || self.own_awaited(slot, root) {
                Block::Pending
            } else {
                Block::Opaque
            };
        };
        let bitmap = |has: bool, len: u16, bytes: &[u8; BITMAP_BYTES]| {
            has.then(|| bytes.get(..usize::from(len)).unwrap_or_default().to_vec())
        };
        match event.kind {
            BLOCK_MISSING => Block::Missing,
            BLOCK_FOOTER => Block::Footer(Box::new(FooterFacts {
                close_nanos: event.close_nanos,
                notar_bitmap: bitmap(event.has_notar, event.notar_len, &event.notar),
                skip_bitmap: bitmap(event.has_skip, event.skip_len, &event.skip),
                fast: event.has_fast.then_some(event.fast),
            })),
            _ => Block::Opaque,
        }
    }
    fn slot_leader(&self, slot: Slot, bank: &dyn BankView) -> Option<Pubkey> {
        let state = self.state.read().unwrap();
        Self::leader_of(&state, bank.epoch_schedule(), slot)
    }
    fn next_leader_slot(
        &self,
        me: &Pubkey,
        from: Slot,
        bank: &dyn BankView,
        range: u64,
    ) -> Option<(Slot, Slot)> {
        let state = self.state.read().unwrap();
        let schedule = bank.epoch_schedule();
        let stride = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
        let mut slot = from.saturating_add(1);
        let end = from.saturating_add(range);
        let mut first = None;
        while slot <= end {
            // Past the schedules held, a run already found ends where they end.
            let Some(leader) = Self::leader_of(&state, schedule, slot) else {
                return first.map(|start| (start, slot.saturating_sub(1)));
            };
            match (first, leader == *me) {
                (None, true) => first = Some(slot),
                (Some(start), false) => return Some((start, slot.saturating_sub(1))),
                _ => {}
            }
            // Within a turn the leader stays, so step to the turn's end once it is decided.
            let (_, index) = schedule.get_epoch_and_slot_index(slot);
            let into_turn = index.checked_rem(stride).unwrap_or(0);
            slot = slot.saturating_add(stride.saturating_sub(into_turn));
        }
        first.map(|start| (start, end))
    }
    fn epoch_turn_leaders(&self, epoch: Epoch) -> Option<Vec<Pubkey>> {
        let state = self.state.read().unwrap();
        state.schedules.get(&epoch).map(|leaders| leaders.to_vec())
    }
    fn leader_slot_indexes(&self, epoch: Epoch, leader: &Pubkey, slots: u64) -> Option<Vec<u64>> {
        let state = self.state.read().unwrap();
        let stride = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
        let leaders = state.schedules.get(&epoch)?;
        Some(
            leaders
                .iter()
                .zip(0u64..)
                .filter(|(key, _)| *key == leader)
                .flat_map(|(_, turn)| {
                    let first = turn.saturating_mul(stride);
                    first..first.saturating_add(stride)
                })
                .take_while(|index| *index < slots)
                .collect(),
        )
    }
    fn snapshot_config(&self) -> Option<SnapshotConfig> {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .and_then(|node| node.snapshot_config.clone())
    }
    fn snapshots(&self) -> Option<snapshot::Snapshots> {
        self.snapshot_config().as_ref().and_then(snapshot::read)
    }
    fn account_paths(&self) -> Vec<PathBuf> {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .map_or_else(Vec::new, |node| node.account_paths.clone())
    }
    fn validator_info_all(&self) -> (Slot, Vec<(Pubkey, ValidatorInfo)>) {
        let mut state = self.state.write().unwrap();
        // Handed over whole here, so not again as changes.
        state.info_changes.clear();
        let slot = state
            .info
            .values()
            .map(|(slot, _)| *slot)
            .max()
            .unwrap_or(0);
        let entries = state
            .info
            .iter()
            .map(|(identity, (_, info))| (*identity, info.clone()))
            .collect();
        (slot, entries)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{events::ROLE_WORKING, fixture::fixture, source::Direct},
        solana_core::node_events::output::{clock_event, epoch_event, node_event, status_event},
    };

    fn block_event(slot: Slot, block: Block) -> BlockEvent {
        use solana_core::node_events::blocks as published;
        let block = match block {
            Block::Footer(facts) => published::Block::Footer(Box::new(published::FooterFacts {
                close_nanos: facts.close_nanos,
                notar_bitmap: facts.notar_bitmap,
                skip_bitmap: facts.skip_bitmap,
                fast: facts.fast,
            })),
            Block::Missing => published::Block::Missing,
            Block::Pending => published::Block::Pending,
            Block::Opaque => published::Block::Opaque,
        };
        solana_core::node_events::output::block_event(slot, block)
    }

    fn wait_part(
        generation: u64,
        index: u32,
        count: u32,
        stake: u64,
        seen: bool,
    ) -> SupermajorityWaitEvent {
        let mut version = [0u8; solana_core::start_events::VERSION_BYTES];
        version[..5].copy_from_slice(b"4.5.0");
        SupermajorityWaitEvent {
            pid: 1,
            tid: 1,
            timestamp_ns: 0,
            generation,
            index,
            count,
            slot: 77,
            shred_version: 9,
            online_stake: 30,
            total_activated_stake: 60,
            identity: Pubkey::new_unique().to_bytes(),
            activated_stake: stake,
            seen,
            version,
        }
    }

    #[test]
    fn test_start_progress_carries_the_validators_timings() {
        let progress = Arc::new(RwLock::new(ValidatorStartProgress::Initializing));
        let mirror = Mirror::new(progress.clone());
        let mut taken_phases = [0u8; solana_core::start_events::PHASES];
        let mut taken_ns = [0u64; solana_core::start_events::PHASES];
        taken_phases[0] = 5;
        taken_ns[0] = 90;
        mirror.apply_progress(&StartProgressEvent {
            pid: 0,
            tid: 0,
            timestamp_ns: 42,
            phase: 6,
            slot: 150,
            max_slot: 200,
            gossip_stake_percent: 0,
            phase_elapsed_ns: 7,
            has_replay_origin: true,
            replay_origin: 100,
            taken_len: 1,
            taken_phases,
            taken_ns,
        });
        assert_eq!(
            *progress.read().unwrap(),
            ValidatorStartProgress::ProcessingLedger {
                slot: 150,
                max_slot: 200
            }
        );
        assert!(mirror.progress_reported());
        let timings = mirror.reported().read().unwrap().timings.clone().unwrap();
        assert_eq!(timings.phase, "processing_ledger");
        assert_eq!((timings.phase_elapsed_nanos, timings.at_ns), (7, 42));
        assert_eq!(timings.replay_origin, Some(100));
        assert_eq!(timings.taken.len(), 1);
        assert_eq!(timings.taken[0].phase, "loading_ledger");
        assert_eq!(timings.taken[0].elapsed_nanos, 90);
    }

    #[test]
    fn test_a_whole_wait_check_rebuilds_the_wait_panel() {
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Initializing)));
        let parts = [
            wait_part(3, 0, 3, 10, true),
            wait_part(3, 1, 3, 30, false),
            wait_part(3, 2, 3, 20, true),
        ];
        // A part of an older check is dropped by the newer one.
        mirror.apply_wait(&wait_part(2, 0, 3, 99, true));
        mirror.apply_wait(&parts[0]);
        mirror.apply_wait(&parts[1]);
        assert!(mirror.reported().read().unwrap().gossip.is_none());
        mirror.apply_wait(&parts[2]);
        let gossip = mirror.reported().read().unwrap().gossip.clone().unwrap();
        assert_eq!((gossip.slot, gossip.shred_version), (77, 9));
        assert_eq!((gossip.total, gossip.seen), (60, 30));
        let stakes: Vec<_> = gossip
            .validators
            .iter()
            .map(|v| (v.stake, v.seen))
            .collect();
        assert_eq!(stakes, [(30, false), (20, true), (10, true)]);
        assert_eq!(gossip.validators[1].version.as_deref(), Some("4.5.0"));
        assert_eq!(gossip.validators[0].version, None);
    }

    #[test]
    fn test_a_resubscribed_wait_drops_its_partial_check() {
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Initializing)));
        mirror.apply_wait(&wait_part(4, 0, 2, 10, true));
        mirror.reset_set(SetStream::Wait);
        mirror.apply_wait(&wait_part(4, 1, 2, 20, true));
        assert!(mirror.reported().read().unwrap().gossip.is_none());
    }

    #[test]
    fn test_the_mirror_answers_as_the_validator_does() {
        let harness = fixture();
        harness.advance_to(5);
        let direct = Direct::new(harness.ctx.clone(), None);
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let (identity, vote_account) = (direct.identity(), direct.vote_account());
        mirror.apply_node(&node_event(&harness.reader(), 0));
        assert_eq!(mirror.caught_up_millis(), None);
        mirror.apply_node(&node_event(&harness.reader(), 1_234));
        assert_eq!(mirror.caught_up_millis(), Some(1_234));
        mirror.apply_status(&status_event(&harness.reader()));
        let working = direct.working_bank();
        mirror.apply_epoch(&epoch_event(working.as_bank().unwrap(), &vote_account));
        mirror.apply_clock(&clock_event(
            ROLE_WORKING,
            working.as_bank().unwrap(),
            &identity,
            &vote_account,
        ));
        let root = direct.root_bank();
        mirror.apply_clock(&clock_event(
            ROLE_ROOT,
            root.as_bank().unwrap(),
            &identity,
            &vote_account,
        ));
        // Not ready without the vote accounts, schedule and slot history.
        assert!(!mirror.ready());

        assert_eq!(mirror.identity(), identity);
        assert_eq!(mirror.vote_account(), vote_account);
        assert_eq!(mirror.cluster_name(), direct.cluster_name());
        assert_eq!(mirror.shred_version(), direct.shred_version());
        assert_eq!(
            PeerRow {
                wallclock: 0,
                ..mirror.my_contact()
            },
            PeerRow {
                wallclock: 0,
                ..direct.my_contact()
            }
        );
        assert_eq!(mirror.commitment(), direct.commitment());
        assert_eq!(mirror.cluster_tip(), direct.cluster_tip());
        assert_eq!(mirror.highest_slot(), direct.highest_slot());
        assert_eq!(mirror.lowest_slot(), direct.lowest_slot());
        assert_eq!(mirror.ledger_path(), direct.ledger_path());
        assert_eq!(mirror.account_paths(), direct.account_paths());
        for (theirs, ours) in [(mirror.working_bank(), working), (mirror.root_bank(), root)] {
            assert_eq!(theirs.slot(), ours.slot());
            assert_eq!(theirs.parent_slot(), ours.parent_slot());
            assert_eq!(theirs.epoch(), ours.epoch());
            assert_eq!(theirs.epoch_schedule(), ours.epoch_schedule());
            assert_eq!(theirs.is_alpenglow(), ours.is_alpenglow());
            assert_eq!(theirs.is_frozen(), ours.is_frozen());
            assert_eq!(theirs.block_height(), ours.block_height());
            assert_eq!(theirs.clock(), ours.clock());
            assert_eq!(theirs.last_blockhash(), ours.last_blockhash());
            assert_eq!(theirs.transaction_count(), ours.transaction_count());
            assert_eq!(theirs.cost(), ours.cost());
            assert_eq!(theirs.fees(), ours.fees());
            assert_eq!(theirs.get_balance(&identity), ours.get_balance(&identity));
            assert_eq!(
                theirs.get_balance(&vote_account),
                ours.get_balance(&vote_account)
            );
            assert_eq!(
                theirs.get_lamports_per_signature(),
                ours.get_lamports_per_signature()
            );
            assert_eq!(
                theirs.minimum_vote_account_balance_for_vat(),
                ours.minimum_vote_account_balance_for_vat()
            );
        }
    }

    #[test]
    fn test_rank_maps_and_slot_history_come_back_whole() {
        use solana_core::node_events::output::{admitted_events, history_events};
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let entries: Vec<RankEntry> = (0..70u64)
            .map(|stake| RankEntry {
                vote: Pubkey::new_unique(),
                identity: Pubkey::new_unique(),
                stake: stake.saturating_add(1),
            })
            .collect();
        let view = EpochStakeView {
            total_stake: 9_999,
            ranks: Arc::new(RankRows::new(entries.clone())),
        };
        let published = solana_core::node_events::bank_view::EpochStakeView {
            total_stake: view.total_stake,
            ranks: Arc::new(solana_core::node_events::bank_view::RankRows::new(
                entries
                    .iter()
                    .map(|entry| solana_core::node_events::bank_view::RankEntry {
                        vote: entry.vote,
                        identity: entry.identity,
                        stake: entry.stake,
                    })
                    .collect(),
            )),
        };
        let mut events = admitted_events(0, 1, &published);
        assert_eq!(events.len(), 3);
        // Out of order, and a stale generation's part left over.
        events.reverse();
        mirror.apply_admitted(&admitted_events(0, 0, &published)[0]);
        for event in &events {
            mirror.apply_admitted(event);
        }
        let bank = mirror.working_bank();
        let ranks = bank.rank_map(5).unwrap();
        assert_eq!(ranks.len(), 70);
        assert_eq!(ranks.entry(69), entries.last().copied());
        assert_eq!(ranks.rank_of(&entries[3].vote), Some(3));
        assert_eq!(bank.epoch_stakes(0).unwrap().total_stake, 9_999);

        let mut history = SlotHistory::default();
        for slot in [1, 2, 5, 9, 4_000] {
            history.add(slot);
        }
        for event in history_events(&history, 1) {
            mirror.apply_history(&event);
        }
        mirror.apply_rooted(4_000);
        mirror.apply_rooted(4_002);
        let held = mirror.root_bank().get_slot_history().unwrap();
        history.add(4_002);
        assert_eq!(held, history);
    }

    fn frozen(slot: Slot) -> BankFrozenEvent {
        BankFrozenEvent {
            pid: 0,
            tid: 0,
            timestamp_ns: 0,
            slot,
            bank_id: 0,
            parent_slot: slot.saturating_sub(1),
            leader: [0u8; 32],
            blockhash: [0u8; 32],
            transaction_count: 0,
            non_vote_transaction_count: 0,
            failed_transaction_count: 0,
            transaction_entry_count: 0,
            block_cost: 0,
            block_cost_limit: 0,
            account_cost_limit: 0,
            total_fee: 0,
            priority_fee: 0,
            block_height: 0,
            transaction_count_total: 0,
            non_vote_transaction_count_total: 0,
        }
    }

    fn reading(slot: Slot) -> FinalizationEvent {
        FinalizationEvent {
            pid: 0,
            tid: 0,
            timestamp_ns: 0,
            slot,
            front: 0,
            ranks: 0,
            seen: [0u8; crate::events::FINALIZATION_RANKS / 8],
            carried: [0u8; crate::events::FINALIZATION_RANKS / 8],
        }
    }

    #[test]
    fn test_a_frozen_bank_waits_for_its_reading_and_counts_nothing_without_it() {
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let rows_of = |bank: &Arc<dyn BankView>| {
            let mut rows = 0;
            bank.for_each_vote_row(&mut |_| rows += 1);
            rows
        };
        let mut account = crate::event_input::tests_support::vote_account(1);
        account.has_last_voted_slot = true;
        mirror.apply_votes(std::iter::once(&account));
        // Before any reading has arrived, banks go straight through and count the vote accounts.
        mirror.apply_frozen(&frozen(1));
        let taken = mirror.take_frozen().unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(rows_of(&taken[0]), 1);

        mirror.apply_finalization(&reading(2));
        mirror.apply_frozen(&frozen(2));
        mirror.apply_frozen(&frozen(3));
        let taken = mirror.take_frozen().unwrap();
        assert_eq!(
            taken.iter().map(|bank| bank.slot()).collect::<Vec<_>>(),
            vec![2]
        );

        std::thread::sleep(FINALIZATION_WAIT);
        let taken = mirror.take_frozen().unwrap();
        assert_eq!(
            taken.iter().map(|bank| bank.slot()).collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(
            rows_of(&taken[0]),
            0,
            "a bank without its reading counts nobody"
        );

        // With the stream off again, banks stop waiting and count the vote accounts.
        mirror.reset_finalization();
        mirror.apply_frozen(&frozen(4));
        let taken = mirror.take_frozen().unwrap();
        assert_eq!(rows_of(&taken[0]), 1);
    }

    #[test]
    fn test_a_full_info_set_leaves_a_newer_change_alone() {
        use solana_core::node_events::output::info_events;
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let key = Pubkey::new_unique();
        let named = |name: &str| solana_core::node_events::info::ValidatorInfo {
            name: Some(name.to_string()),
            icon_url: None,
        };
        for event in info_events(crate::events::INFO_CHANGE, 1, 20, &[(key, named("new"))]) {
            mirror.apply_info(&event);
        }
        for event in info_events(INFO_FULL_SET, 1, 10, &[(key, named("old"))]) {
            mirror.apply_info(&event);
        }
        let (_, entries) = mirror.validator_info_all();
        assert_eq!(
            entries,
            vec![(
                key,
                ValidatorInfo {
                    name: Some("new".to_string()),
                    icon_url: None,
                }
            )]
        );
    }

    #[test]
    fn test_roots_before_the_history_arrives_are_kept() {
        use solana_core::node_events::output::history_events;
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let mut history = SlotHistory::default();
        history.add(10);
        mirror.apply_rooted(12);
        for event in history_events(&history, 1) {
            mirror.apply_history(&event);
        }
        history.add(12);
        assert_eq!(mirror.root_bank().get_slot_history().unwrap(), history);
    }

    #[test]
    fn test_a_set_restarted_under_the_same_generation_completes_from_its_new_parts() {
        let mut pending = None;
        assert!(PendingSet::add(&mut pending, 1, 0, 3, 'a').is_none());
        // A resubscribe drops the partial set; the new queue starts at generation 1 again.
        pending = None;
        assert!(PendingSet::add(&mut pending, 1, 1, 2, 'x').is_none());
        assert_eq!(
            PendingSet::add(&mut pending, 1, 0, 2, 'y'),
            Some(vec!['y', 'x'])
        );
    }

    #[test]
    fn test_turn_leaders_answer_slot_and_next_leader_lookups() {
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let (me, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let schedule = EpochSchedule::custom(32, 32, false);
        // Eight turns of four slots: other, me, me, other...
        let leaders = [other, me, me, other, other, other, me, other];
        mirror.apply_schedule(&EpochInfo {
            epoch: 0,
            start_slot: 0,
            end_slot: 31,
            slots_in_epoch: 32,
            my_leader_slots: Vec::new(),
            leaders: vec![other.to_string(), me.to_string()],
            turns: leaders.iter().map(|key| u16::from(*key == me)).collect(),
            block_cost_limit: 0,
            account_cost_limit: 0,
        });
        let state = mirror.state.read().unwrap();
        assert_eq!(Mirror::leader_of(&state, &schedule, 5), Some(me));
        assert_eq!(Mirror::leader_of(&state, &schedule, 12), Some(other));
        assert_eq!(Mirror::leader_of(&state, &schedule, 40), None);
        drop(state);
        assert_eq!(
            mirror.leader_slot_indexes(0, &me, 32),
            Some((4..12).chain(24..28).collect())
        );
        mirror.apply_epoch(&EpochEvent {
            slots_per_epoch: 32,
            leader_schedule_slot_offset: 32,
            warmup: false,
            first_normal_epoch: 0,
            first_normal_slot: 0,
            ..empty_epoch()
        });
        let bank = mirror.working_bank();
        assert_eq!(
            mirror.next_leader_slot(&me, 0, bank.as_ref(), 100),
            Some((4, 11))
        );
        assert_eq!(
            mirror.next_leader_slot(&me, 12, bank.as_ref(), 100),
            Some((24, 27))
        );
        assert_eq!(
            mirror.next_leader_slot(&me, 25, bank.as_ref(), 100),
            Some((26, 27))
        );
        // Epoch 1's schedule has not arrived.
        assert_eq!(mirror.next_leader_slot(&me, 27, bank.as_ref(), 100), None);
    }

    /// A mirror whose node leads turns 1, 2 and 6 of a 32-slot epoch 0, with the own block
    /// stream connected.
    fn own_mirror() -> (Mirror, Pubkey) {
        let harness = fixture();
        let direct = Direct::new(harness.ctx.clone(), None);
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        mirror.apply_node(&node_event(&harness.reader(), 0));
        let (me, other) = (direct.identity(), Pubkey::new_unique());
        let leaders = [other, me, me, other, other, other, me, other];
        mirror.apply_schedule(&EpochInfo {
            epoch: 0,
            start_slot: 0,
            end_slot: 31,
            slots_in_epoch: 32,
            my_leader_slots: Vec::new(),
            leaders: vec![other.to_string(), me.to_string()],
            turns: leaders.iter().map(|key| u16::from(*key == me)).collect(),
            block_cost_limit: 0,
            account_cost_limit: 0,
        });
        mirror.apply_epoch(&EpochEvent {
            slots_per_epoch: 32,
            leader_schedule_slot_offset: 32,
            warmup: false,
            first_normal_epoch: 0,
            first_normal_slot: 0,
            ..empty_epoch()
        });
        mirror.set_own_blocks_connected(true, true);
        (mirror, me)
    }

    fn footer(close_nanos: u64) -> Block {
        Block::Footer(Box::new(FooterFacts {
            close_nanos,
            notar_bitmap: Some(vec![1, 2]),
            skip_bitmap: None,
            fast: Some(true),
        }))
    }

    fn own_set(
        epoch: Epoch,
        me: Pubkey,
        generation: u64,
        blocks: &[(Slot, Block)],
    ) -> Vec<OwnBlockEvent> {
        let blocks: Vec<BlockEvent> = blocks
            .iter()
            .map(|(slot, block)| {
                let block = match block {
                    Block::Footer(facts) => Block::Footer(facts.clone()),
                    Block::Missing => Block::Missing,
                    Block::Pending => Block::Pending,
                    Block::Opaque => Block::Opaque,
                };
                block_event(*slot, block)
            })
            .collect();
        solana_core::node_events::output::own_block_events((epoch, me), generation, 20, &blocks)
    }

    #[test]
    fn test_our_blocks_wait_for_their_set_and_no_others_do() {
        let (mirror, _) = own_mirror();
        assert!(matches!(mirror.block(5, 20), Block::Pending));
        assert!(matches!(mirror.block(12, 20), Block::Opaque));
        // Ours, but in the epoch before the root's.
        assert!(matches!(mirror.block(5, 40), Block::Opaque));
        // A block sent at its root answers while the set is awaited.
        mirror.apply_block(&block_event(5, footer(7)));
        assert!(matches!(mirror.block(5, 20), Block::Footer(_)));
        assert!(matches!(mirror.block(4, 20), Block::Pending));
        mirror.set_own_blocks_connected(false, false);
        assert!(matches!(mirror.block(4, 20), Block::Opaque));
    }

    #[test]
    fn test_a_whole_own_set_answers_for_our_blocks() {
        let (mirror, me) = own_mirror();
        let mut set = own_set(0, me, 1, &[(4, footer(9)), (5, Block::Missing)]);
        assert_eq!(
            set.iter().map(|part| part.count).collect::<Vec<_>>(),
            [2, 2]
        );
        assert_eq!(set[1].covered_to, 20);
        let last = set.pop().unwrap();
        mirror.apply_own_block(&set[0]);
        assert!(matches!(mirror.block(6, 20), Block::Pending));
        // A resubscribed stream drops the partial set.
        mirror.reset_set(SetStream::OwnBlocks);
        mirror.apply_own_block(&last);
        assert!(matches!(mirror.block(6, 20), Block::Pending));
        for part in own_set(0, me, 2, &[(4, footer(9)), (5, Block::Missing)]) {
            mirror.apply_own_block(&part);
        }
        match mirror.block(4, 20) {
            Block::Footer(facts) => {
                assert_eq!(facts.close_nanos, 9);
                assert_eq!(facts.notar_bitmap, Some(vec![1, 2]));
                assert_eq!(facts.fast, Some(true));
            }
            _ => panic!("expected a footer"),
        }
        assert!(matches!(mirror.block(5, 20), Block::Missing));
        // Ours, but not in the set.
        assert!(matches!(mirror.block(6, 20), Block::Opaque));
    }

    #[test]
    fn test_only_this_epochs_set_for_our_identity_ends_the_wait() {
        let (mirror, me) = own_mirror();
        for part in own_set(0, Pubkey::new_unique(), 1, &[]) {
            mirror.apply_own_block(&part);
        }
        assert!(matches!(mirror.block(5, 20), Block::Pending));
        for part in own_set(1, me, 2, &[]) {
            mirror.apply_own_block(&part);
        }
        assert!(matches!(mirror.block(5, 20), Block::Pending));
        let empty = own_set(0, me, 3, &[]);
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].count, 0);
        mirror.apply_own_block(&empty[0]);
        assert!(matches!(mirror.block(5, 20), Block::Opaque));
    }

    #[test]
    fn test_our_blocks_stop_waiting_after_a_while() {
        let (mirror, _) = own_mirror();
        assert!(matches!(mirror.block(5, 20), Block::Pending));
        std::thread::sleep(OWN_SET_WAIT);
        assert!(matches!(mirror.block(5, 20), Block::Opaque));
    }

    #[test]
    fn test_a_block_comes_back_whole_through_an_own_set() {
        let block = block_event(4, footer(3));
        let place = crate::events::OwnSetPlace {
            pid: block.pid,
            tid: block.tid,
            timestamp_ns: block.timestamp_ns,
            epoch: 2,
            identity: [7; 32],
            generation: 1,
            index: 0,
            count: 1,
            covered_to: 4,
        };
        assert_eq!(OwnBlockEvent::new(place, &block).block(), block);
    }

    #[test]
    fn test_a_slot_past_the_newest_block_sent_waits_for_it_a_while() {
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        assert!(matches!(mirror.block(9, 20), Block::Opaque));
        mirror.apply_block(&block_event(10, Block::Missing));
        assert!(matches!(mirror.block(9, 20), Block::Opaque));
        assert!(matches!(mirror.block(11, 20), Block::Pending));
        std::thread::sleep(BLOCKS_STALE);
        assert!(matches!(mirror.block(11, 20), Block::Opaque));
    }
}
