//! The node's state as events: who it is, the epoch, vote accounts, leader schedules and rank
//! maps (the previous, this and the next epoch's), and the slot history, each resent in full
//! every 5 seconds for subscribers that join late.
//! Members of a set share a `generation`, and `index` and `count` mark when it is complete.
//! Vote accounts are read from the working bank, and each tick sends those that changed,
//! last vote included.

use {
    crate::node_events::types::{
        ACCOUNT_PATHS, ADMITTED_STREAM, AdmittedEvent, EPOCH_STREAM, EpochEvent, HISTORY_WORDS,
        NODE_INFO_STREAM, NodeInfoEvent, PATH_BYTES, RANKS_PER_EVENT, SLOT_HISTORY_STREAM,
        SlotHistoryEvent, VOTE_REWARD_BYTES, pack_contact, pack_path, pack_str,
    },
    agave_event_system::{
        EventSystem, StreamConfig, event,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
    agave_snapshots::{SnapshotInterval, snapshot_config::SnapshotConfig},
    solana_clock::{Epoch, Slot},
    solana_cluster_type::ClusterType,
    solana_gossip::cluster_info::ClusterInfo,
    solana_leader_schedule::{LeaderSchedule, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::{Bank, VATHealthError},
        bank_forks::BankForks,
        epoch_stakes::BLSPubkeyToRankMap,
    },
    solana_slot_history::SlotHistory,
    solana_vote_interface::state::VoteStateV4,
    std::{
        collections::HashMap,
        path::PathBuf,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub const VOTE_ACCOUNT_STREAM: StreamName = agave_event_system::stream_name!("bank.vote_account");
pub const LEADER_SCHEDULE_STREAM: StreamName =
    agave_event_system::stream_name!("bank.leader_schedule");

pub const VOTE_ACCOUNT_FULL_SET: u8 = 0;
pub const VOTE_ACCOUNT_CHANGE: u8 = 1;

/// Turn leaders per `Turns` event.
pub const TURNS_PER_EVENT: usize = 256;

const TICK: Duration = Duration::from_secs(1);
const FULL_SET_INTERVAL: Duration = Duration::from_secs(5);
/// Where the runtime keeps each epoch's inflation: `vote_reward_account` under the alpenglow
/// feature id.
pub const VOTE_REWARD_ACCOUNT: Pubkey =
    Pubkey::from_str_const("3vJzniWALu2qGFJ1ZY6JD3Y2ZmA5zWqmVdDX7yBXHgu7");
const PACE_BATCH: usize = 512;
const PACE_PAUSE: Duration = Duration::from_millis(50);

/// One vote account read from the working bank: staked accounts and our own.
/// The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct VoteAccountEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// The bank the account was read from.
    pub slot: Slot,
    pub epoch: Epoch,
    pub generation: u64,
    /// Position in a full set, or in this tick's changes.
    pub index: u32,
    pub count: u32,
    /// `VOTE_ACCOUNT_FULL_SET` or `VOTE_ACCOUNT_CHANGE`.
    pub reason: u8,
    pub vote_account: [u8; 32],
    pub identity: [u8; 32],
    /// In lamports.
    pub activated_stake: u64,
    pub commission: u8,
    pub inflation_rewards_commission_bps: u16,
    /// The vote state's newest epoch credits entry.
    pub has_credits: bool,
    pub credits_epoch: Epoch,
    pub credits: u64,
    pub prev_credits: u64,
    pub has_bls_key: bool,
    pub has_last_voted_slot: bool,
    pub last_voted_slot: Slot,
    /// In lamports.
    pub pending_delegator_rewards: u64,
}

/// One epoch's leader schedule: each distinct leader once, then each turn as an index into them.
/// The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
// Queue cells take the largest variant's size whatever the Rust layout.
#[allow(clippy::large_enum_variant)]
pub enum LeaderScheduleEvent {
    Leader {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        epoch: Epoch,
        generation: u64,
        index: u32,
        count: u32,
        identity: [u8; 32],
    },
    Turns {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        epoch: Epoch,
        generation: u64,
        first_slot: Slot,
        first_turn: u32,
        turns_in_epoch: u32,
        /// Turns used in `leaders`, from the start.
        len: u16,
        leaders: [u16; TURNS_PER_EVENT],
    },
}

/// The fields whose change sends an account between full sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AccountKey {
    identity: Pubkey,
    stake: u64,
    commission: u8,
    has_bls_key: bool,
    last_voted_slot: Option<Slot>,
    credits: u64,
}

impl AccountKey {
    fn of(event: &VoteAccountEvent) -> Self {
        Self {
            identity: Pubkey::new_from_array(event.identity),
            stake: event.activated_stake,
            commission: event.commission,
            has_bls_key: event.has_bls_key,
            last_voted_slot: event.has_last_voted_slot.then_some(event.last_voted_slot),
            credits: event.credits,
        }
    }
}

/// Staked vote accounts and our own, sorted by vote account, as a full set.
pub(crate) fn vote_account_events(
    bank: &Bank,
    our_vote_account: &Pubkey,
    generation: u64,
) -> Vec<VoteAccountEvent> {
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let epoch = bank.epoch();
    let accounts = bank.vote_accounts();
    let mut members: Vec<_> = accounts
        .iter()
        .filter(|(key, (stake, _))| *stake > 0 || *key == our_vote_account)
        .collect();
    members.sort_unstable_by_key(|(key, _)| **key);
    let count = u32::try_from(members.len()).unwrap_or(u32::MAX);
    members
        .into_iter()
        .zip(0u32..)
        .map(|((key, (stake, account)), index)| {
            let view = account.vote_state_view();
            let last_voted_slot = view.last_voted_slot();
            let credits = view.epoch_credits_iter().last();
            VoteAccountEvent {
                pid,
                tid,
                timestamp_ns,
                slot: bank.slot(),
                epoch,
                generation,
                index,
                count,
                reason: VOTE_ACCOUNT_FULL_SET,
                vote_account: key.to_bytes(),
                identity: account.node_pubkey().to_bytes(),
                activated_stake: *stake,
                commission: view.commission(),
                inflation_rewards_commission_bps: view.inflation_rewards_commission(),
                has_credits: credits.is_some(),
                credits_epoch: credits.map_or(0, |item| item.epoch()),
                credits: credits.map_or(0, |item| item.credits()),
                prev_credits: credits.map_or(0, |item| item.prev_credits()),
                has_bls_key: view.bls_pubkey_compressed().is_some(),
                has_last_voted_slot: last_voted_slot.is_some(),
                last_voted_slot: last_voted_slot.unwrap_or_default(),
                pending_delegator_rewards: view.pending_delegator_rewards(),
            }
        })
        .collect()
}

/// The members of `set` whose key differs from `sent`, renumbered as changes.
fn changed_accounts(
    set: Vec<VoteAccountEvent>,
    sent: &HashMap<[u8; 32], AccountKey>,
) -> Vec<VoteAccountEvent> {
    let mut changes: Vec<_> = set
        .into_iter()
        .filter(|event| sent.get(&event.vote_account) != Some(&AccountKey::of(event)))
        .collect();
    let count = u32::try_from(changes.len()).unwrap_or(u32::MAX);
    for (event, index) in changes.iter_mut().zip(0u32..) {
        event.index = index;
        event.count = count;
        event.reason = VOTE_ACCOUNT_CHANGE;
    }
    changes
}

/// `None` when the schedule does not cover the epoch in whole turns, or has more leaders than
/// a `u16` indexes.
pub(crate) fn leader_schedule_events(
    schedule: &LeaderSchedule,
    epoch: Epoch,
    first_slot: Slot,
    slots_in_epoch: u64,
    generation: u64,
) -> Option<Vec<LeaderScheduleEvent>> {
    let stride = NUM_CONSECUTIVE_LEADER_SLOTS.get();
    let mut leaders: Vec<Pubkey> = Vec::new();
    let mut seen: HashMap<Pubkey, u16> = HashMap::new();
    let mut turns: Vec<u16> = Vec::new();
    for leader in schedule.get_slot_leaders().step_by(stride) {
        let index = match seen.get(&leader.id) {
            Some(index) => *index,
            None => {
                let index = u16::try_from(leaders.len()).ok()?;
                leaders.push(leader.id);
                seen.insert(leader.id, index);
                index
            }
        };
        turns.push(index);
    }
    if (turns.len() as u64).saturating_mul(stride as u64) != slots_in_epoch {
        return None;
    }

    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let count = u32::try_from(leaders.len()).ok()?;
    let turns_in_epoch = u32::try_from(turns.len()).ok()?;
    let mut events: Vec<LeaderScheduleEvent> = leaders
        .iter()
        .zip(0u32..)
        .map(|(identity, index)| LeaderScheduleEvent::Leader {
            pid,
            tid,
            timestamp_ns,
            epoch,
            generation,
            index,
            count,
            identity: identity.to_bytes(),
        })
        .collect();
    for (chunk, chunk_index) in turns.chunks(TURNS_PER_EVENT).zip(0u32..) {
        let mut packed = [0u16; TURNS_PER_EVENT];
        packed[..chunk.len()].copy_from_slice(chunk);
        events.push(LeaderScheduleEvent::Turns {
            pid,
            tid,
            timestamp_ns,
            epoch,
            generation,
            first_slot,
            first_turn: chunk_index.saturating_mul(TURNS_PER_EVENT as u32),
            turns_in_epoch,
            len: u16::try_from(chunk.len()).unwrap_or(u16::MAX),
            leaders: packed,
        });
    }
    Some(events)
}

/// What `node.info` carries besides the banks.
pub struct NodeFacts {
    pub cluster_info: Arc<ClusterInfo>,
    pub ledger_path: PathBuf,
    pub account_paths: Vec<PathBuf>,
    pub snapshot_config: Option<SnapshotConfig>,
}

pub fn node_event(facts: &NodeFacts, bank: &Bank, vote_account: &Pubkey) -> NodeInfoEvent {
    let (tid, pid) = cached_thread_and_process_id();
    let contact = facts.cluster_info.my_contact_info();
    let mut account_paths = [0u8; ACCOUNT_PATHS * PATH_BYTES];
    for (packed, path) in account_paths
        .as_chunks_mut::<PATH_BYTES>()
        .0
        .iter_mut()
        .zip(&facts.account_paths)
    {
        packed.copy_from_slice(&pack_path(path));
    }
    let snapshots = facts.snapshot_config.as_ref();
    let slots = |interval: SnapshotInterval| match interval {
        SnapshotInterval::Disabled => 0,
        SnapshotInterval::Slots(slots) => slots.get(),
    };
    NodeInfoEvent {
        pid,
        tid,
        timestamp_ns: monotonic_timestamp_ns(),
        identity: contact.pubkey().to_bytes(),
        vote_account: vote_account.to_bytes(),
        cluster: match bank.cluster_type() {
            ClusterType::Testnet => 0,
            ClusterType::MainnetBeta => 1,
            ClusterType::Devnet => 2,
            ClusterType::Development => 3,
        },
        shred_version: facts.cluster_info.my_shred_version(),
        version: pack_str(&contact.version().to_string()),
        client: pack_str(&contact.version().client().to_string()),
        wallclock: 0,
        outset: contact.outset(),
        addrs: pack_contact(&contact),
        ledger_path: pack_path(&facts.ledger_path),
        account_paths,
        has_snapshots: snapshots.is_some(),
        snapshots_generated: snapshots.is_some_and(|config| config.should_generate_snapshots()),
        full_snapshot_dir: snapshots.map_or([0u8; PATH_BYTES], |config| {
            pack_path(&config.full_snapshot_archives_dir)
        }),
        incremental_snapshot_dir: snapshots.map_or([0u8; PATH_BYTES], |config| {
            pack_path(&config.incremental_snapshot_archives_dir)
        }),
        full_snapshot_interval: snapshots
            .map_or(0, |config| slots(config.full_snapshot_archive_interval)),
        incremental_snapshot_interval: snapshots.map_or(0, |config| {
            slots(config.incremental_snapshot_archive_interval)
        }),
        global_addresses_only: matches!(
            facts.cluster_info.socket_addr_space(),
            solana_net_utils::SocketAddrSpace::Global
        ),
    }
}

pub fn epoch_event(bank: &Bank, vote_account: &Pubkey) -> EpochEvent {
    let (tid, pid) = cached_thread_and_process_id();
    let schedule = bank.epoch_schedule();
    let (block_cost_limit, account_cost_limit) =
        bank.read_cost_tracker().map_or((0, 0), |tracker| {
            (tracker.get_block_limit(), tracker.get_account_limit())
        });
    let ticket_short = match bank.get_vat_health_for_next_epoch(vote_account) {
        Err(VATHealthError::InsufficientFundsInVoteAccount(balance, minimum)) => {
            Some(minimum.saturating_sub(balance))
        }
        Ok(()) | Err(_) => None,
    };
    let vote_reward = bank
        .get_account(&VOTE_REWARD_ACCOUNT)
        .map(|account| solana_account::ReadableAccount::data(&account).to_vec())
        .filter(|data| data.len() <= VOTE_REWARD_BYTES)
        .unwrap_or_default();
    let mut packed_reward = [0u8; VOTE_REWARD_BYTES];
    if let Some(slot) = packed_reward.get_mut(..vote_reward.len()) {
        slot.copy_from_slice(&vote_reward);
    }
    EpochEvent {
        pid,
        tid,
        timestamp_ns: monotonic_timestamp_ns(),
        epoch: bank.epoch(),
        slots_per_epoch: schedule.slots_per_epoch,
        leader_schedule_slot_offset: schedule.leader_schedule_slot_offset,
        warmup: schedule.warmup,
        first_normal_epoch: schedule.first_normal_epoch,
        first_normal_slot: schedule.first_normal_slot,
        ns_per_slot: u64::try_from(bank.ns_per_slot_at_slot(bank.slot())).unwrap_or(u64::MAX),
        alpenglow: bank.is_alpenglow(),
        lamports_per_signature: bank.get_lamports_per_signature(),
        block_cost_limit,
        account_cost_limit,
        minimum_vat_balance: bank.minimum_vote_account_balance_for_vat(),
        vote_account_rent: bank.get_minimum_balance_for_rent_exemption(VoteStateV4::size_of()),
        has_ticket_short: ticket_short.is_some(),
        ticket_short: ticket_short.unwrap_or(0),
        vote_reward_len: u8::try_from(vote_reward.len()).unwrap_or(0),
        vote_reward: packed_reward,
    }
}

/// An epoch's rank map in parts of `RANKS_PER_EVENT`.
pub fn admitted_events(
    epoch: Epoch,
    generation: u64,
    total_stake: u64,
    ranks: &BLSPubkeyToRankMap,
) -> Vec<AdmittedEvent> {
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let len = ranks.len();
    let count = u32::try_from(len.div_ceil(RANKS_PER_EVENT)).unwrap_or(u32::MAX);
    (0..len)
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
                total_stake,
                first_rank: u32::try_from(first).unwrap_or(u32::MAX),
                len: 0,
                votes: [0u8; RANKS_PER_EVENT * 32],
                identities: [0u8; RANKS_PER_EVENT * 32],
                stakes: [0u64; RANKS_PER_EVENT],
            };
            let entries = (first..len.min(first.saturating_add(RANKS_PER_EVENT)))
                .filter_map(|rank| ranks.get_pubkey_stake_entry(rank));
            for (at, entry) in entries.enumerate() {
                let start = at.saturating_mul(32);
                if let Some(slot) = event.votes.get_mut(start..start.saturating_add(32)) {
                    slot.copy_from_slice(&entry.vote_account_pubkey.to_bytes());
                }
                if let Some(slot) = event.identities.get_mut(start..start.saturating_add(32)) {
                    slot.copy_from_slice(&entry.node_pubkey.to_bytes());
                }
                if let Some(slot) = event.stakes.get_mut(at) {
                    *slot = entry.stake.get().get();
                }
                event.len = event.len.saturating_add(1);
            }
            event
        })
        .collect()
}

/// The slot history's bitmap in parts of `HISTORY_WORDS`.
pub fn history_events(history: &SlotHistory, generation: u64) -> Vec<SlotHistoryEvent> {
    use bv::Bits;
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
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

/// Publishes in batches with a pause between, so a set larger than the stream's capacity arrives
/// whole when the subscriber drains within the pauses.
fn publish_paced<E: agave_event_system::Event>(
    publisher: &mut Publisher<E>,
    events: &[E],
    exit: &AtomicBool,
) {
    for (batch_index, batch) in events.chunks(PACE_BATCH).enumerate() {
        if batch_index > 0 {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            thread::sleep(PACE_PAUSE);
        }
        for event in batch {
            let _ = publisher.publish(event);
        }
    }
}

/// Everything but the header, which decides whether an event changed.
trait Unstamped: Clone + PartialEq {
    fn unstamped(&self) -> Self;
}

macro_rules! unstamped {
    ($($event:ty),*) => {$(
        impl Unstamped for $event {
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

unstamped!(NodeInfoEvent, EpochEvent);

/// Sends an event when it changes, when its stream turns on, and with each full set.
struct Latest<E: agave_event_system::Event + Unstamped> {
    publisher: Publisher<E>,
    sent: Option<E>,
}

impl<E: agave_event_system::Event + Unstamped> Latest<E> {
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

    fn offer(&mut self, event: E, due: bool) {
        let key = event.unstamped();
        if due || self.sent.as_ref() != Some(&key) {
            let _ = self.publisher.publish(&event);
            self.sent = Some(key);
        }
    }
}

fn publisher<E: agave_event_system::Event>(
    event_system: &EventSystem,
    name: StreamName,
    capacity: usize,
) -> Result<Publisher<E>, String> {
    let shown = format!("{name:?}");
    event_system
        .create_stream::<E>(
            name,
            StreamConfig {
                capacity,
                publisher_slots: 1,
                subscriber_slots: 8,
            },
        )
        .map_err(|err| format!("Failed to create stream {shown}: {err}"))?
        .try_create_publisher()
        .ok_or_else(|| format!("No publisher slot on the new stream {shown}"))
}

/// The streams besides vote accounts and leader schedules.
struct StatePublishers {
    node: Latest<NodeInfoEvent>,
    epoch: Latest<EpochEvent>,
    admitted: Publisher<AdmittedEvent>,
    history: Publisher<SlotHistoryEvent>,
}

pub struct BankStateEvents {
    thread_hdl: JoinHandle<()>,
}

impl BankStateEvents {
    pub fn new(
        event_system: &EventSystem,
        bank_forks: Arc<RwLock<BankForks>>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        our_vote_account: Pubkey,
        node: NodeFacts,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, String> {
        let mut state_publishers = StatePublishers {
            node: Latest::new(publisher(event_system, NODE_INFO_STREAM, 16)?),
            epoch: Latest::new(publisher(event_system, EPOCH_STREAM, 16)?),
            admitted: publisher(event_system, ADMITTED_STREAM, 1024)?,
            history: publisher(event_system, SLOT_HISTORY_STREAM, 256)?,
        };
        let vote_factory = event_system
            .create_stream::<VoteAccountEvent>(
                VOTE_ACCOUNT_STREAM,
                StreamConfig {
                    capacity: 4096,
                    publisher_slots: 1,
                    subscriber_slots: 8,
                },
            )
            .map_err(|err| format!("Failed to create vote account stream: {err}"))?;
        let schedule_factory = event_system
            .create_stream::<LeaderScheduleEvent>(
                LEADER_SCHEDULE_STREAM,
                StreamConfig {
                    capacity: 2048,
                    publisher_slots: 1,
                    subscriber_slots: 8,
                },
            )
            .map_err(|err| format!("Failed to create leader schedule stream: {err}"))?;
        let thread_hdl = Builder::new()
            .name("solEvtState".to_string())
            .spawn(move || {
                let mut vote_publisher = vote_factory
                    .try_create_publisher()
                    .expect("fresh vote account stream has one publisher slot");
                let mut schedule_publisher = schedule_factory
                    .try_create_publisher()
                    .expect("fresh leader schedule stream has one publisher slot");
                let mut state = StateLoop::default();
                while !exit.load(Ordering::Relaxed) {
                    state.tick(
                        &mut vote_publisher,
                        &mut schedule_publisher,
                        &bank_forks,
                        &leader_schedule_cache,
                        &our_vote_account,
                        &exit,
                    );
                    state.tick_state(
                        &mut state_publishers,
                        &bank_forks,
                        &node,
                        &our_vote_account,
                        &exit,
                    );
                    thread::sleep(TICK);
                }
            })
            .map_err(|err| format!("Failed to spawn solEvtState: {err}"))?;
        Ok(Self { thread_hdl })
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[derive(Default)]
struct StateLoop {
    admitted_generation: u64,
    history_generation: u64,
    admitted_on: bool,
    history_on: bool,
    /// The full-set clock for the streams `tick_state` sends.
    state_epoch: Option<Epoch>,
    state_next_full: Option<Instant>,
    vote_generation: u64,
    schedule_generation: u64,
    votes_on: bool,
    schedule_on: bool,
    last_epoch: Option<Epoch>,
    next_full: Option<Instant>,
    sent: HashMap<[u8; 32], AccountKey>,
}

impl StateLoop {
    fn tick(
        &mut self,
        vote_publisher: &mut Publisher<VoteAccountEvent>,
        schedule_publisher: &mut Publisher<LeaderScheduleEvent>,
        bank_forks: &RwLock<BankForks>,
        leader_schedule_cache: &LeaderScheduleCache,
        our_vote_account: &Pubkey,
        exit: &AtomicBool,
    ) {
        let votes_on = vote_publisher.is_enabled();
        let schedule_on = schedule_publisher.is_enabled();
        let votes_turned_on = votes_on && !self.votes_on;
        let schedule_turned_on = schedule_on && !self.schedule_on;
        self.votes_on = votes_on;
        self.schedule_on = schedule_on;
        if !votes_on && !schedule_on {
            self.sent.clear();
            return;
        }

        let bank = bank_forks.read().unwrap().working_bank();
        let epoch = bank.epoch();
        let now = Instant::now();
        let due = self.last_epoch != Some(epoch) || self.next_full.is_none_or(|at| now >= at);
        if due {
            self.last_epoch = Some(epoch);
            self.next_full = Some(now + FULL_SET_INTERVAL);
        }

        if votes_on {
            let full = due || votes_turned_on;
            if full {
                self.vote_generation = self.vote_generation.saturating_add(1);
                self.sent.clear();
            }
            // Changes carry the generation of the full set they amend.
            let set = vote_account_events(&bank, our_vote_account, self.vote_generation);
            let events = if full {
                set
            } else {
                changed_accounts(set, &self.sent)
            };
            for event in &events {
                self.sent.insert(event.vote_account, AccountKey::of(event));
            }
            publish_paced(vote_publisher, &events, exit);
        }

        if schedule_on && (due || schedule_turned_on) {
            self.schedule_generation = self.schedule_generation.saturating_add(1);
            let generation = self.schedule_generation;
            let epoch_schedule = bank.epoch_schedule();
            for epoch in [epoch.saturating_sub(1), epoch, epoch.saturating_add(1)] {
                let Some(schedule) = leader_schedule_cache.get_epoch_leader_schedule(epoch) else {
                    continue;
                };
                let first_slot = epoch_schedule.get_first_slot_in_epoch(epoch);
                let slots_in_epoch = epoch_schedule.get_slots_in_epoch(epoch);
                if let Some(events) =
                    leader_schedule_events(&schedule, epoch, first_slot, slots_in_epoch, generation)
                {
                    publish_paced(schedule_publisher, &events, exit);
                }
            }
        }
    }
}

impl StateLoop {
    /// Node info and the epoch when they change, rank maps and the slot history in full; all at
    /// turn-on, a new epoch and every `FULL_SET_INTERVAL`.
    fn tick_state(
        &mut self,
        out: &mut StatePublishers,
        bank_forks: &RwLock<BankForks>,
        node: &NodeFacts,
        our_vote_account: &Pubkey,
        exit: &AtomicBool,
    ) {
        let node_on = out.node.enabled();
        let epoch_on = out.epoch.enabled();
        let admitted_on = out.admitted.is_enabled();
        let history_on = out.history.is_enabled();
        let admitted_turned_on = admitted_on && !self.admitted_on;
        let history_turned_on = history_on && !self.history_on;
        self.admitted_on = admitted_on;
        self.history_on = history_on;
        if !(node_on || epoch_on || admitted_on || history_on) {
            return;
        }

        let (working, root) = {
            let bank_forks = bank_forks.read().unwrap();
            (bank_forks.working_bank(), bank_forks.root_bank())
        };
        let epoch = working.epoch();
        let now = Instant::now();
        let due =
            self.state_epoch != Some(epoch) || self.state_next_full.is_none_or(|at| now >= at);
        if due {
            self.state_epoch = Some(epoch);
            self.state_next_full = Some(now + FULL_SET_INTERVAL);
        }

        if node_on {
            out.node
                .offer(node_event(node, &working, our_vote_account), due);
        }
        if epoch_on {
            out.epoch
                .offer(epoch_event(&working, our_vote_account), due);
        }
        if admitted_on && (due || admitted_turned_on) {
            self.admitted_generation = self.admitted_generation.saturating_add(1);
            let generation = self.admitted_generation;
            let events: Vec<AdmittedEvent> =
                [epoch.saturating_sub(1), epoch, epoch.saturating_add(1)]
                    .into_iter()
                    .filter_map(|epoch| Some((epoch, working.epoch_stakes(epoch)?)))
                    .flat_map(|(epoch, stakes)| {
                        admitted_events(
                            epoch,
                            generation,
                            stakes.total_stake(),
                            stakes.bls_pubkey_to_rank_map(),
                        )
                    })
                    .collect();
            publish_paced(&mut out.admitted, &events, exit);
        }
        if history_on
            && (due || history_turned_on)
            && let Some(history) = root.get_slot_history()
        {
            self.history_generation = self.history_generation.saturating_add(1);
            publish_paced(
                &mut out.history,
                &history_events(&history, self.history_generation),
                exit,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_leader_schedule::SlotLeader,
        solana_runtime::genesis_utils::{
            ValidatorVoteKeypairs, create_genesis_config_with_vote_accounts,
        },
        solana_signer::Signer,
    };

    #[test]
    fn test_rank_map_goes_out_with_every_rank_and_the_epoch_with_its_schedule() {
        let keypairs: Vec<_> = (0..3).map(|_| ValidatorVoteKeypairs::new_rand()).collect();
        let genesis =
            create_genesis_config_with_vote_accounts(1_000_000_000, &keypairs, vec![100, 200, 300]);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        let stakes = bank.epoch_stakes(bank.epoch()).unwrap();
        let ranks = stakes.bls_pubkey_to_rank_map();
        assert_eq!(ranks.len(), 3);

        let events = admitted_events(0, 4, stakes.total_stake(), ranks);
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(
            (event.generation, event.index, event.count, event.len),
            (4, 0, 1, 3)
        );
        assert_eq!(event.total_stake, stakes.total_stake());
        let sent = event
            .votes
            .as_chunks::<32>()
            .0
            .iter()
            .zip(event.stakes)
            .take(3);
        for (rank, (vote, stake)) in sent.enumerate() {
            let entry = ranks.get_pubkey_stake_entry(rank).unwrap();
            assert_eq!(*vote, entry.vote_account_pubkey.to_bytes());
            assert_eq!(stake, entry.stake.get().get());
        }

        let ours = keypairs[0].vote_keypair.pubkey();
        let epoch = epoch_event(&bank, &ours);
        assert_eq!(epoch.epoch, bank.epoch());
        assert_eq!(epoch.slots_per_epoch, bank.epoch_schedule().slots_per_epoch);
        assert_eq!(epoch.alpenglow, bank.is_alpenglow());
    }

    #[test]
    fn test_vote_account_set_counts_staked_and_ours() {
        let keypairs: Vec<_> = (0..3).map(|_| ValidatorVoteKeypairs::new_rand()).collect();
        let genesis =
            create_genesis_config_with_vote_accounts(1_000_000_000, &keypairs, vec![100, 200, 300]);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        let ours = keypairs[1].vote_keypair.pubkey();

        let set = vote_account_events(&bank, &ours, 7);
        assert_eq!(set.len(), 3);
        for (event, index) in set.iter().zip(0u32..) {
            assert_eq!((event.index, event.count, event.generation), (index, 3, 7));
            assert_eq!(event.reason, VOTE_ACCOUNT_FULL_SET);
        }
        let mine = set
            .iter()
            .find(|event| event.vote_account == ours.to_bytes())
            .unwrap();
        assert_eq!(mine.identity, keypairs[1].node_keypair.pubkey().to_bytes());
        assert_eq!(mine.activated_stake, 200);

        let mut sent: HashMap<_, _> = set
            .iter()
            .map(|event| (event.vote_account, AccountKey::of(event)))
            .collect();
        assert!(changed_accounts(vote_account_events(&bank, &ours, 7), &sent).is_empty());
        sent.get_mut(&ours.to_bytes()).unwrap().stake = 1;
        let changes = changed_accounts(vote_account_events(&bank, &ours, 7), &sent);
        assert_eq!(changes.len(), 1);
        assert_eq!(
            (changes[0].index, changes[0].count, changes[0].reason),
            (0, 1, VOTE_ACCOUNT_CHANGE)
        );
    }

    #[test]
    fn test_leader_schedule_events_reassemble() {
        let ids: Vec<_> = (0..3).map(|_| SlotLeader::new_unique()).collect();
        // 300 turns, so the turns span two events.
        let turn_leaders: Vec<SlotLeader> = (0..300).map(|turn| ids[turn % 3 / 2]).collect();
        let schedule =
            LeaderSchedule::new_from_schedule(turn_leaders, NUM_CONSECUTIVE_LEADER_SLOTS);
        let slots = 300 * NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;

        assert!(leader_schedule_events(&schedule, 5, 0, slots + 4, 1).is_none());
        let events = leader_schedule_events(&schedule, 5, 0, slots, 1).unwrap();
        let mut leaders = Vec::new();
        let mut turns = vec![u16::MAX; 300];
        for event in &events {
            match event {
                LeaderScheduleEvent::Leader {
                    index,
                    count,
                    identity,
                    ..
                } => {
                    assert_eq!(*count, 2);
                    assert_eq!(*index as usize, leaders.len());
                    leaders.push(Pubkey::new_from_array(*identity));
                }
                LeaderScheduleEvent::Turns {
                    first_turn,
                    turns_in_epoch,
                    len,
                    leaders: packed,
                    ..
                } => {
                    assert_eq!(*turns_in_epoch, 300);
                    let first = *first_turn as usize;
                    let len = *len as usize;
                    turns[first..first + len].copy_from_slice(&packed[..len]);
                }
            }
        }
        let expected: Vec<Pubkey> = schedule
            .get_slot_leaders()
            .step_by(NUM_CONSECUTIVE_LEADER_SLOTS.get())
            .map(|leader| leader.id)
            .collect();
        let rebuilt: Vec<Pubkey> = turns.iter().map(|index| leaders[*index as usize]).collect();
        assert_eq!(rebuilt, expected);
    }
}
