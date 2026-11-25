//! The node rebuilt from event streams, read through the same [`Source`] the in-process
//! collector reads. Test branch only. What no stream carries yet reads as empty.

use {
    crate::{
        certs::Block,
        collect::{EpochInfo, TxOutcome},
        context::StartProgress,
        events::{
            ACCOUNT_PATHS, ADDR_ALPENGLOW, ADDR_BYTES, ADDR_GOSSIP, ADDR_RPC,
            ADDR_SERVE_REPAIR_UDP, ADDR_TPU_FORWARDS_QUIC, ADDR_TPU_QUIC, ADDR_TPU_VOTE_QUIC,
            ADDR_TPU_VOTE_UDP, ADDR_TVU_UDP, EpochEvent, NodeInfoEvent, PATH_BYTES, ROLE_ROOT,
            SlotClockEvent, SlotStatusEvent, StartProgressEvent, unpack_addr, unpack_path,
            unpack_str,
        },
        snapshot,
        source::{
            BankView, Banks, Commitment, CostFacts, EpochStakeView, FeeTotals, PeerRow, Ranks,
            Source, VoteRow,
        },
        validator_info::ValidatorInfo,
        versions::TxVersions,
    },
    agave_snapshots::{
        SnapshotInterval,
        snapshot_config::{SnapshotConfig, SnapshotUsage},
    },
    solana_clock::{Clock, Epoch, Slot},
    solana_core::{bank_state_events::VoteAccountEvent, validator::ValidatorStartProgress},
    solana_epoch_schedule::EpochSchedule,
    solana_hash::Hash,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_slot_history::SlotHistory,
    std::{
        collections::{BTreeMap, HashMap},
        net::SocketAddr,
        num::NonZeroU64,
        path::PathBuf,
        str::FromStr,
        sync::{Arc, RwLock},
    },
};

/// Epoch schedules kept: the previous, this and the next.
const EPOCHS_KEPT: usize = 3;

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
        }
    }
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
    fn rank_map(&self, _slot: Slot) -> Option<Arc<dyn Ranks>> {
        None
    }
    fn epoch_stakes(&self, _epoch: Epoch) -> Option<EpochStakeView> {
        None
    }
    fn epoch_stakes_from_slot(&self, _slot: Slot) -> Option<EpochStakeView> {
        None
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
        None
    }
    fn account_data(&self, _key: &Pubkey) -> Option<Vec<u8>> {
        None
    }
    fn signature_status_slot(&self, _signature: &Signature) -> Option<Slot> {
        None
    }
    fn validator_info_changed(&self) -> Vec<(Pubkey, ValidatorInfo)> {
        Vec::new()
    }
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
}

pub struct Mirror {
    state: RwLock<State>,
    start_progress: StartProgress,
}

impl Mirror {
    pub fn new(start_progress: StartProgress) -> Self {
        Self {
            state: RwLock::new(State::default()),
            start_progress,
        }
    }

    /// Whether enough has arrived for the collector to start.
    pub fn ready(&self) -> bool {
        let state = self.state.read().unwrap();
        state.node.is_some()
            && state.status.is_some()
            && state.epoch.is_some()
            && state.root.is_some()
            && state.working.is_some()
    }

    pub fn apply_node(&self, event: &NodeInfoEvent) {
        self.state.write().unwrap().node = Some(Node::from(event));
    }

    pub fn apply_progress(&self, event: &StartProgressEvent) {
        let progress = match event.phase {
            1 => ValidatorStartProgress::SearchingForRpcService,
            2 => ValidatorStartProgress::DownloadingSnapshot {
                slot: event.slot,
                rpc_addr: unpack_addr(&event.rpc_addr)
                    .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0))),
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
        *self.start_progress.write().unwrap() = progress;
    }

    pub fn apply_status(&self, event: &SlotStatusEvent) {
        self.state.write().unwrap().status = Some(event.clone());
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
        let Some(leaders) = info
            .turns
            .iter()
            .map(|index| {
                info.leaders
                    .get(usize::from(*index))
                    .and_then(|key| Pubkey::from_str(key).ok())
            })
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
        })
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
        Vec::new()
    }
    fn peer_gossip(&self, key: &Pubkey) -> Option<SocketAddr> {
        let state = self.state.read().unwrap();
        state
            .node
            .as_ref()
            .filter(|node| node.identity == *key)
            .and_then(|node| node.contact.gossip)
    }
    fn snapshot_slots(&self, keys: &[Pubkey]) -> Vec<(Option<Slot>, Option<Slot>)> {
        vec![(None, None); keys.len()]
    }
    fn rpc_node_count(&self, _peers: &[(PeerRow, u64)]) -> usize {
        0
    }
    fn banks(&self) -> Banks {
        let state = self.state.read().unwrap();
        Banks {
            root: self.bank(&state, state.root.as_ref()),
            working: self.bank(&state, state.working.as_ref()),
            highest_slot: state
                .status
                .as_ref()
                .map_or(0, |status| status.highest_slot),
            frozen: Vec::new(),
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
        let state = self.state.read().unwrap();
        state
            .status
            .as_ref()
            .map_or(0, |status| status.highest_slot)
    }
    fn take_frozen(&self) -> Option<Vec<Arc<dyn BankView>>> {
        Some(Vec::new())
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
    fn first_shred_time(&self, _slot: Slot) -> Option<u64> {
        None
    }
    fn parent_of(&self, _slot: Slot) -> Option<Slot> {
        None
    }
    fn is_root(&self, _slot: Slot) -> bool {
        false
    }
    fn is_full(&self, _slot: Slot) -> bool {
        false
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
    fn message_versions(&self, _slot: Slot) -> Option<TxVersions> {
        None
    }
    fn tx_outcomes(
        &self,
        _slot: Slot,
        _bank: &dyn BankView,
        _ours: bool,
        _followed: &dyn Fn(&Pubkey) -> bool,
    ) -> Option<Vec<TxOutcome>> {
        None
    }
    fn block(&self, slot: Slot, root: Slot) -> Block {
        if slot > root {
            Block::Pending
        } else {
            Block::Opaque
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
            let leader = Self::leader_of(&state, schedule, slot)?;
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
        (0, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            event_output::{clock_event, epoch_event, node_event, status_event},
            events::ROLE_WORKING,
            fixture::fixture,
            source::Direct,
        },
    };

    #[test]
    fn test_the_mirror_answers_as_the_validator_does() {
        let harness = fixture();
        harness.advance_to(5);
        let direct = Direct::new(harness.ctx.clone(), None);
        let mirror = Mirror::new(Arc::new(RwLock::new(ValidatorStartProgress::Running)));
        let (identity, vote_account) = (direct.identity(), direct.vote_account());
        mirror.apply_node(&node_event(&direct));
        mirror.apply_status(&status_event(&direct));
        let working = direct.working_bank();
        mirror.apply_epoch(&epoch_event(working.as_ref(), &vote_account));
        mirror.apply_clock(&clock_event(
            ROLE_WORKING,
            working.as_ref(),
            &identity,
            &vote_account,
        ));
        let root = direct.root_bank();
        mirror.apply_clock(&clock_event(
            ROLE_ROOT,
            root.as_ref(),
            &identity,
            &vote_account,
        ));
        assert!(mirror.ready());

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
    }
}
