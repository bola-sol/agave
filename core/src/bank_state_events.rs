//! Vote accounts and leader schedules (the previous, this and the next epoch's) as sets of
//! events, resent in full every 5 seconds for subscribers that join late.
//! Members of a set share a `generation`, and `index` and `count` mark when it is complete.
//! Vote accounts are read from the working bank, and each tick sends those that changed,
//! last vote included.

use {
    agave_event_system::{
        EventSystem, StreamConfig, event,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
    solana_clock::{Epoch, Slot},
    solana_leader_schedule::{LeaderSchedule, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    std::{
        collections::HashMap,
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

pub struct BankStateEvents {
    thread_hdl: JoinHandle<()>,
}

impl BankStateEvents {
    pub fn new(
        event_system: &EventSystem,
        bank_forks: Arc<RwLock<BankForks>>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        our_vote_account: Pubkey,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, String> {
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
