//! Dashboard input read from the validator's event streams by a dashboard in its own process.
//! Test branch only: the streams are this branch's, not Anza's.

use {
    crate::{
        collect::{
            CATCH_UP_SLOTS_PER_SECOND, EpochInfo, ValidatorCounts, is_delinquent, system_time_nanos,
        },
        meters::{CHART_HISTORY, Tps, TpsSample},
        proto::{Debounced, Publisher, TOPIC_SUMMARY},
    },
    agave_event_system::{
        Event,
        event_metadata::monotonic_timestamp_ns,
        stream_name::StreamName,
        subscriber::{StreamExplorer, Subscriber, Typed},
        wincode::Deserialize,
    },
    solana_clock::{Epoch, Slot},
    solana_core::{
        bank_state_events::{
            LEADER_SCHEDULE_STREAM, LeaderScheduleEvent, VOTE_ACCOUNT_CHANGE, VOTE_ACCOUNT_STREAM,
            VoteAccountEvent,
        },
        replay_stage::events::{BANK_FROZEN_STREAM, BankFrozenEvent, SLOT_EVENT_STREAM, SlotEvent},
        shred_fetch_events::{SHRED_FETCH_PATH_REPAIR, SHRED_FETCH_STREAM, ShredFetchCounters},
    },
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        ffi::OsString,
        fmt::Write as _,
        path::{Path, PathBuf},
        sync::{Arc, RwLock},
        time::{Duration, Instant, SystemTime},
    },
};

/// The monotonic clock time an event was stamped with.
pub trait Stamped {
    fn timestamp_ns(&self) -> u64;
}

impl Stamped for SlotEvent {
    fn timestamp_ns(&self) -> u64 {
        match self {
            Self::Begin { timestamp_ns, .. }
            | Self::ExecutionFinished { timestamp_ns, .. }
            | Self::Removed { timestamp_ns, .. }
            | Self::Rooted { timestamp_ns, .. } => *timestamp_ns,
        }
    }
}

impl Stamped for BankFrozenEvent {
    fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }
}

impl Stamped for ShredFetchCounters {
    fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }
}

impl Stamped for VoteAccountEvent {
    fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }
}

impl Stamped for LeaderScheduleEvent {
    fn timestamp_ns(&self) -> u64 {
        match self {
            Self::Leader { timestamp_ns, .. } | Self::Turns { timestamp_ns, .. } => *timestamp_ns,
        }
    }
}

#[derive(Debug, Default)]
pub struct StreamStats {
    pub received: u64,
    pub decode_errors: u64,
    /// Monotonic nanoseconds from publish to receipt, for the newest event.
    pub last_lag_ns: u64,
    pub max_lag_ns: u64,
    pub connects: u64,
    pub connect_error: Option<String>,
}

/// One typed subscription, reconnected after the validator recreates its event directory.
pub struct Stream<E> {
    name: StreamName,
    subscriber: Option<Subscriber<Typed<E>>>,
    /// The queue file the subscription was made to; the validator makes a new one each time the
    /// stream is enabled or the validator restarts.
    queue: Option<OsString>,
    pub stats: StreamStats,
}

/// The stream's current queue file, `None` while the stream is off.
fn queue_name(dir: &Path, stream: &str) -> Option<OsString> {
    std::fs::read_dir(dir.join("event-streams").join(stream))
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .find(|name| name.to_string_lossy().starts_with("queue-"))
}

impl<E> Stream<E>
where
    E: Event + Stamped + for<'de> Deserialize<'de, Dst = E>,
{
    fn new(name: StreamName) -> Self {
        Self {
            name,
            subscriber: None,
            queue: None,
            stats: StreamStats::default(),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn connected(&self) -> bool {
        self.subscriber.is_some()
    }

    /// True when a new subscription was made.
    fn connect(&mut self, dir: &Path, explorer: &StreamExplorer) -> bool {
        let queue = queue_name(dir, self.name.as_str());
        if self.subscriber.is_some() && queue == self.queue {
            return false;
        }
        self.disconnect();
        if queue.is_none() {
            return false;
        }
        let Some(stream) = explorer
            .available_streams()
            .find(|stream| stream.stream_name().as_str() == self.name.as_str())
        else {
            return false;
        };
        match stream.try_connect_typed::<E>() {
            Ok(subscriber) => {
                self.subscriber = Some(subscriber);
                self.queue = queue;
                self.stats.connects = self.stats.connects.saturating_add(1);
                self.stats.connect_error = None;
                true
            }
            Err(err) => {
                self.stats.connect_error = Some(err.to_string());
                false
            }
        }
    }

    fn disconnect(&mut self) {
        self.subscriber = None;
        self.queue = None;
    }

    /// Hands every waiting event to `handle`.
    fn drain(&mut self, mut handle: impl FnMut(E)) {
        let Some(subscriber) = self.subscriber.as_mut() else {
            return;
        };
        while let Ok(message) = subscriber.try_recv() {
            match message.decode() {
                Ok(event) => {
                    self.stats.received = self.stats.received.saturating_add(1);
                    let lag = monotonic_timestamp_ns().saturating_sub(event.timestamp_ns());
                    self.stats.last_lag_ns = lag;
                    self.stats.max_lag_ns = self.stats.max_lag_ns.max(lag);
                    handle(event);
                }
                Err(_) => self.stats.decode_errors = self.stats.decode_errors.saturating_add(1),
            }
        }
    }

    /// Rejected events per lane, counted by the publisher.
    pub fn rejected(&self) -> Vec<u64> {
        self.subscriber
            .as_ref()
            .map_or_else(Vec::new, |subscriber| {
                subscriber
                    .lanes_metadata()
                    .iter()
                    .map(|lane| lane.rejected_items())
                    .collect()
            })
    }
}

/// Vote accounts from complete sets, with changes applied between them.
#[derive(Debug, Default)]
pub struct VoteSet {
    current: Option<HashMap<[u8; 32], VoteAccountEvent>>,
    pending: Option<(u64, u32, BTreeMap<u32, VoteAccountEvent>)>,
    pub sets_completed: u64,
    pub sets_abandoned: u64,
}

impl VoteSet {
    /// True when the accounts in force changed.
    pub fn apply(&mut self, event: VoteAccountEvent) -> bool {
        if event.reason == VOTE_ACCOUNT_CHANGE {
            return match self.current.as_mut() {
                Some(current) => {
                    current.insert(event.vote_account, event);
                    true
                }
                None => false,
            };
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|(generation, _, _)| *generation != event.generation)
        {
            self.pending = None;
            self.sets_abandoned = self.sets_abandoned.saturating_add(1);
        }
        let (_, count, members) = self
            .pending
            .get_or_insert_with(|| (event.generation, event.count, BTreeMap::new()));
        let count = *count;
        members.insert(event.index, event);
        if members.len() as u64 != u64::from(count) {
            return false;
        }
        let Some((_, _, members)) = self.pending.take() else {
            return false;
        };
        self.current = Some(
            members
                .into_values()
                .map(|event| (event.vote_account, event))
                .collect(),
        );
        self.sets_completed = self.sets_completed.saturating_add(1);
        true
    }

    /// Drops a partial set, whose generations a new queue numbers afresh.
    pub fn reset_pending(&mut self) {
        self.pending = None;
    }

    pub fn accounts(&self) -> Option<impl Iterator<Item = &VoteAccountEvent>> {
        self.current.as_ref().map(|current| current.values())
    }
}

/// The in-process tally, except each account is judged against the bank it was read from,
/// since a set's members are read up to 30 seconds apart.
pub fn validator_counts<'a>(
    accounts: impl Iterator<Item = &'a VoteAccountEvent>,
) -> ValidatorCounts {
    let mut staked: HashSet<[u8; 32]> = HashSet::new();
    let mut delinquent: HashSet<[u8; 32]> = HashSet::new();
    let mut delinquent_stake = 0u64;
    let mut non_delinquent_stake = 0u64;
    for account in accounts.filter(|account| account.activated_stake > 0) {
        let last_vote = account
            .has_last_voted_slot
            .then_some(account.last_voted_slot);
        if is_delinquent(last_vote, account.slot) {
            delinquent.insert(account.identity);
            delinquent_stake = delinquent_stake.saturating_add(account.activated_stake);
        } else {
            non_delinquent_stake = non_delinquent_stake.saturating_add(account.activated_stake);
        }
        staked.insert(account.identity);
    }
    ValidatorCounts {
        total: staked.len(),
        delinquent: delinquent.len(),
        rpc_nodes: 0,
        non_delinquent_stake,
        delinquent_stake,
    }
}

#[derive(Debug)]
struct PendingSchedule {
    generation: u64,
    first_slot: Option<Slot>,
    leaders: Vec<Option<[u8; 32]>>,
    turns: Vec<Option<u16>>,
    leaders_filled: usize,
    turns_filled: usize,
}

impl PendingSchedule {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            first_slot: None,
            leaders: Vec::new(),
            turns: Vec::new(),
            leaders_filled: 0,
            turns_filled: 0,
        }
    }

    fn complete(&self) -> Option<EpochInfo> {
        let first_slot = self.first_slot?;
        if self.leaders.is_empty()
            || self.turns.is_empty()
            || self.leaders_filled != self.leaders.len()
            || self.turns_filled != self.turns.len()
        {
            return None;
        }
        let leaders: Option<Vec<String>> = self
            .leaders
            .iter()
            .map(|leader| leader.map(|bytes| Pubkey::new_from_array(bytes).to_string()))
            .collect();
        let turns: Option<Vec<u16>> = self.turns.iter().copied().collect();
        let (leaders, turns) = (leaders?, turns?);
        if turns
            .iter()
            .any(|index| usize::from(*index) >= leaders.len())
        {
            return None;
        }
        let slots_in_epoch =
            (turns.len() as u64).saturating_mul(NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64);
        Some(EpochInfo {
            epoch: 0,
            start_slot: first_slot,
            end_slot: first_slot.saturating_add(slots_in_epoch).saturating_sub(1),
            slots_in_epoch,
            my_leader_slots: Vec::new(),
            leaders,
            turns,
            block_cost_limit: 0,
            account_cost_limit: 0,
        })
    }
}

/// Leader schedules reassembled per epoch.
#[derive(Debug, Default)]
pub struct ScheduleSet {
    pending: HashMap<Epoch, PendingSchedule>,
    pub complete: BTreeMap<Epoch, EpochInfo>,
    pub sets_completed: u64,
}

impl ScheduleSet {
    /// The epoch whose schedule this event completed, if any.
    pub fn apply(&mut self, event: LeaderScheduleEvent) -> Option<Epoch> {
        let (epoch, generation) = match &event {
            LeaderScheduleEvent::Leader {
                epoch, generation, ..
            }
            | LeaderScheduleEvent::Turns {
                epoch, generation, ..
            } => (*epoch, *generation),
        };
        let pending = self
            .pending
            .entry(epoch)
            .or_insert_with(|| PendingSchedule::new(generation));
        if pending.generation != generation {
            *pending = PendingSchedule::new(generation);
        }
        match event {
            LeaderScheduleEvent::Leader {
                index,
                count,
                identity,
                ..
            } => {
                if pending.leaders.len() != count as usize {
                    pending.leaders = vec![None; count as usize];
                    pending.leaders_filled = 0;
                }
                if let Some(slot) = pending.leaders.get_mut(index as usize) {
                    if slot.is_none() {
                        pending.leaders_filled = pending.leaders_filled.saturating_add(1);
                    }
                    *slot = Some(identity);
                }
            }
            LeaderScheduleEvent::Turns {
                first_slot,
                first_turn,
                turns_in_epoch,
                len,
                leaders,
                ..
            } => {
                pending.first_slot = Some(first_slot);
                if pending.turns.len() != turns_in_epoch as usize {
                    pending.turns = vec![None; turns_in_epoch as usize];
                    pending.turns_filled = 0;
                }
                let first = first_turn as usize;
                for (offset, index) in leaders.iter().take(usize::from(len)).enumerate() {
                    if let Some(turn) = pending.turns.get_mut(first.saturating_add(offset)) {
                        if turn.is_none() {
                            pending.turns_filled = pending.turns_filled.saturating_add(1);
                        }
                        *turn = Some(*index);
                    }
                }
            }
        }
        let mut info = pending.complete()?;
        info.epoch = epoch;
        self.pending.remove(&epoch);
        self.sets_completed = self.sets_completed.saturating_add(1);
        let changed = self.complete.get(&epoch) != Some(&info);
        self.complete.insert(epoch, info);
        // Keep this epoch and the next, and no partial set older than them.
        while self.complete.len() > 2 {
            self.complete.pop_first();
        }
        if let Some(oldest) = self.complete.keys().next().copied() {
            self.pending.retain(|epoch, _| *epoch >= oldest);
        }
        changed.then_some(epoch)
    }

    /// Drops partial sets, whose generations a new queue numbers afresh.
    pub fn reset_pending(&mut self) {
        self.pending.clear();
    }
}

/// Per-second rates from frozen banks, as the in-process sampler publishes them.
#[derive(Debug, Default)]
pub struct Throughput {
    total: u64,
    non_vote: u64,
    failed: u64,
    newest_slot: Slot,
    sampled_slot: Slot,
    since: Option<Instant>,
    history: Vec<TpsSample>,
}

impl Throughput {
    pub fn add(&mut self, event: &BankFrozenEvent) {
        self.total = self.total.saturating_add(event.transaction_count);
        self.non_vote = self
            .non_vote
            .saturating_add(event.non_vote_transaction_count);
        self.failed = self.failed.saturating_add(event.failed_transaction_count);
        self.newest_slot = self.newest_slot.max(event.slot);
    }

    /// A sample of the counts since the last call, `None` on the first.
    pub fn sample(&mut self, now: Instant, wall: SystemTime) -> Option<TpsSample> {
        let since = self.since.replace(now);
        let (total, non_vote, failed) = (self.total, self.non_vote, self.failed);
        (self.total, self.non_vote, self.failed) = (0, 0, 0);
        let advanced = self.newest_slot.saturating_sub(self.sampled_slot);
        self.sampled_slot = self.newest_slot;
        let seconds = now.duration_since(since?).as_secs_f64();
        // As in-process: replay catching up would pin the chart's scale.
        if seconds <= 0.0 || advanced as f64 / seconds > CATCH_UP_SLOTS_PER_SECOND {
            return None;
        }
        let total = total as f64 / seconds;
        let non_vote = non_vote as f64 / seconds;
        let failed = failed as f64 / seconds;
        Some(TpsSample {
            slot: self.newest_slot,
            timestamp_nanos: system_time_nanos(wall),
            tps: Tps {
                total,
                vote: (total - non_vote).max(0.0),
                non_vote_success: (non_vote - failed).max(0.0),
                non_vote_failed: failed.min(non_vote),
            },
        })
    }

    pub fn publish(&mut self, sample: TpsSample, publisher: &Publisher) {
        publisher.publish_ephemeral(TOPIC_SUMMARY, "tps_sample", &sample);
        self.history.push(sample);
        if self.history.len() > CHART_HISTORY {
            let excess = self.history.len().saturating_sub(CHART_HISTORY);
            self.history.drain(..excess);
        }
        publisher.retain_only(TOPIC_SUMMARY, "tps_history", &self.history);
    }
}

#[derive(Debug, Default)]
pub struct SlotProgress {
    pub begun: u64,
    pub finished: u64,
    pub failed: u64,
    pub removed: u64,
    pub newest_finished: Slot,
    pub root: Slot,
}

const QUEUE_CHECK: Duration = Duration::from_secs(2);

/// Every subscription and what has been read from them.
pub struct EventInput {
    dir: PathBuf,
    checked: Option<Instant>,
    started: Instant,
    pub slot_events: Stream<SlotEvent>,
    pub bank_frozen: Stream<BankFrozenEvent>,
    pub shred_fetch: Stream<ShredFetchCounters>,
    pub vote_accounts: Stream<VoteAccountEvent>,
    pub schedule: Stream<LeaderScheduleEvent>,
    pub slots: SlotProgress,
    pub throughput: Throughput,
    /// Newest totals per path, turbine first.
    pub shred_totals: [Option<ShredFetchCounters>; 2],
    pub votes: VoteSet,
    pub schedules: ScheduleSet,
    pub first_vote_set: Option<Duration>,
    pub first_schedule: Option<Duration>,
    validator_counts: Debounced<ValidatorCounts>,
    epochs: Arc<RwLock<Vec<EpochInfo>>>,
}

impl EventInput {
    pub fn new(dir: PathBuf, epochs: Arc<RwLock<Vec<EpochInfo>>>) -> Self {
        Self {
            dir,
            checked: None,
            started: Instant::now(),
            slot_events: Stream::new(SLOT_EVENT_STREAM),
            bank_frozen: Stream::new(BANK_FROZEN_STREAM),
            shred_fetch: Stream::new(SHRED_FETCH_STREAM),
            vote_accounts: Stream::new(VOTE_ACCOUNT_STREAM),
            schedule: Stream::new(LEADER_SCHEDULE_STREAM),
            slots: SlotProgress::default(),
            throughput: Throughput::default(),
            shred_totals: [None, None],
            votes: VoteSet::default(),
            schedules: ScheduleSet::default(),
            first_vote_set: None,
            first_schedule: None,
            validator_counts: Debounced::new(TOPIC_SUMMARY, "validator_counts"),
            epochs,
        }
    }

    /// Resubscribes to any stream whose queue changed, after a filter change or a validator
    /// restart.
    pub fn connect(&mut self, now: Instant) {
        if self
            .checked
            .is_some_and(|at| now.duration_since(at) < QUEUE_CHECK)
        {
            return;
        }
        self.checked = Some(now);
        let explorer = StreamExplorer::new(self.dir.clone());
        let dir = self.dir.as_path();
        self.slot_events.connect(dir, &explorer);
        self.bank_frozen.connect(dir, &explorer);
        self.shred_fetch.connect(dir, &explorer);
        if self.vote_accounts.connect(dir, &explorer) {
            self.votes.reset_pending();
        }
        if self.schedule.connect(dir, &explorer) {
            self.schedules.reset_pending();
        }
    }

    pub fn disconnect(&mut self) {
        self.slot_events.disconnect();
        self.bank_frozen.disconnect();
        self.shred_fetch.disconnect();
        self.vote_accounts.disconnect();
        self.schedule.disconnect();
    }

    pub fn drain(&mut self, publisher: &Publisher) {
        let slots = &mut self.slots;
        self.slot_events.drain(|event| match event {
            SlotEvent::Begin { .. } => slots.begun = slots.begun.saturating_add(1),
            SlotEvent::ExecutionFinished {
                slot, succeeded, ..
            } => {
                slots.finished = slots.finished.saturating_add(1);
                if !succeeded {
                    slots.failed = slots.failed.saturating_add(1);
                }
                slots.newest_finished = slots.newest_finished.max(slot);
            }
            SlotEvent::Removed { .. } => slots.removed = slots.removed.saturating_add(1),
            SlotEvent::Rooted { slot, .. } => slots.root = slots.root.max(slot),
        });

        let throughput = &mut self.throughput;
        self.bank_frozen.drain(|event| throughput.add(&event));

        let shred_totals = &mut self.shred_totals;
        self.shred_fetch.drain(|event| {
            let path = usize::from(event.path == SHRED_FETCH_PATH_REPAIR);
            shred_totals[path] = Some(event);
        });

        let votes = &mut self.votes;
        let mut votes_changed = false;
        self.vote_accounts
            .drain(|event| votes_changed |= votes.apply(event));
        if votes_changed {
            if self.first_vote_set.is_none() {
                self.first_vote_set = Some(self.started.elapsed());
            }
            if let Some(accounts) = self.votes.accounts() {
                self.validator_counts
                    .publish(publisher, validator_counts(accounts));
            }
        }

        let schedules = &mut self.schedules;
        let mut completed = Vec::new();
        self.schedule
            .drain(|event| completed.extend(schedules.apply(event)));
        if !completed.is_empty() {
            if self.first_schedule.is_none() {
                self.first_schedule = Some(self.started.elapsed());
            }
            if let Ok(mut epochs) = self.epochs.write() {
                *epochs = self.schedules.complete.values().cloned().collect();
            }
        }
    }

    /// One line per stream for the log.
    pub fn report(&self) -> String {
        let mut out = String::new();
        let mut line = |name: &str, stats: &StreamStats, connected: bool, rejected: Vec<u64>| {
            let _ = writeln!(
                out,
                "events: {name}: connected {connected} ({} times), received {}, decode errors {}, \
                 rejected per lane {rejected:?}, lag ms newest {:.2} max {:.2}{}",
                stats.connects,
                stats.received,
                stats.decode_errors,
                stats.last_lag_ns as f64 / 1e6,
                stats.max_lag_ns as f64 / 1e6,
                stats
                    .connect_error
                    .as_ref()
                    .map(|err| format!(", connect error: {err}"))
                    .unwrap_or_default(),
            );
        };
        line(
            self.slot_events.name(),
            &self.slot_events.stats,
            self.slot_events.connected(),
            self.slot_events.rejected(),
        );
        line(
            self.bank_frozen.name(),
            &self.bank_frozen.stats,
            self.bank_frozen.connected(),
            self.bank_frozen.rejected(),
        );
        line(
            self.shred_fetch.name(),
            &self.shred_fetch.stats,
            self.shred_fetch.connected(),
            self.shred_fetch.rejected(),
        );
        line(
            self.vote_accounts.name(),
            &self.vote_accounts.stats,
            self.vote_accounts.connected(),
            self.vote_accounts.rejected(),
        );
        line(
            self.schedule.name(),
            &self.schedule.stats,
            self.schedule.connected(),
            self.schedule.rejected(),
        );
        let _ = writeln!(
            out,
            "events: slots begun {}, finished {} ({} failed), removed {}, newest finished {}, \
             root {}; shred totals {:?}",
            self.slots.begun,
            self.slots.finished,
            self.slots.failed,
            self.slots.removed,
            self.slots.newest_finished,
            self.slots.root,
            self.shred_totals
                .iter()
                .map(|totals| totals.as_ref().map(|t| (
                    t.packets,
                    t.valid,
                    t.discarded,
                    t.overflow
                )))
                .collect::<Vec<_>>(),
        );
        let _ = write!(
            out,
            "events: vote sets completed {}, abandoned {}, first after {:?}; schedules completed \
             {}, epochs {:?}, first after {:?}",
            self.votes.sets_completed,
            self.votes.sets_abandoned,
            self.first_vote_set,
            self.schedules.sets_completed,
            self.schedules.complete.keys().collect::<Vec<_>>(),
            self.first_schedule,
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_core::bank_state_events::{TURNS_PER_EVENT, VOTE_ACCOUNT_FULL_SET},
    };

    fn account(generation: u64, index: u32, count: u32, key: u8, stake: u64) -> VoteAccountEvent {
        VoteAccountEvent {
            pid: 1,
            tid: 1,
            timestamp_ns: 0,
            slot: 1_000,
            epoch: 3,
            generation,
            index,
            count,
            reason: VOTE_ACCOUNT_FULL_SET,
            vote_account: [key; 32],
            identity: [key; 32],
            activated_stake: stake,
            commission: 0,
            epoch_credits: 0,
            has_bls_key: false,
            has_last_voted_slot: true,
            last_voted_slot: 990,
            pending_delegator_rewards: 0,
        }
    }

    #[test]
    fn test_incomplete_vote_set_is_ignored() {
        let mut set = VoteSet::default();
        assert!(!set.apply(account(1, 0, 2, 1, 10)));
        assert!(set.accounts().is_none());
        // A new generation before the old one finished drops it.
        assert!(!set.apply(account(2, 0, 2, 1, 10)));
        assert!(set.apply(account(2, 1, 2, 2, 20)));
        assert_eq!(set.accounts().unwrap().count(), 2);
        assert_eq!((set.sets_completed, set.sets_abandoned), (1, 1));
    }

    #[test]
    fn test_complete_set_drops_missing_and_changes_apply() {
        let mut set = VoteSet::default();
        set.apply(account(1, 0, 2, 1, 10));
        set.apply(account(1, 1, 2, 2, 20));
        assert!(set.apply(account(2, 0, 1, 2, 20)));
        let keys: Vec<_> = set.accounts().unwrap().map(|a| a.vote_account[0]).collect();
        assert_eq!(keys, vec![2]);

        let mut change = account(2, 0, 1, 2, 25);
        change.reason = VOTE_ACCOUNT_CHANGE;
        assert!(set.apply(change));
        assert_eq!(set.accounts().unwrap().next().unwrap().activated_stake, 25);
    }

    #[test]
    fn test_counts_judge_each_account_by_its_own_bank() {
        let mut stale = account(1, 0, 3, 1, 10);
        stale.last_voted_slot = 800;
        let mut never = account(1, 1, 3, 2, 5);
        never.has_last_voted_slot = false;
        let unstaked = account(1, 2, 3, 3, 0);
        let fresh = account(1, 2, 3, 4, 20);
        let counts = validator_counts([&stale, &never, &unstaked, &fresh].into_iter());
        assert_eq!(
            (
                counts.total,
                counts.delinquent,
                counts.delinquent_stake,
                counts.non_delinquent_stake
            ),
            (3, 2, 15, 20)
        );
    }

    #[test]
    fn test_schedule_reassembles_out_of_order() {
        let turns_in_epoch = (TURNS_PER_EVENT as u32).saturating_add(10);
        let mut first = [0u16; TURNS_PER_EVENT];
        first[1] = 1;
        let mut second = [0u16; TURNS_PER_EVENT];
        second[9] = 1;
        let events = vec![
            LeaderScheduleEvent::Turns {
                pid: 1,
                tid: 1,
                timestamp_ns: 0,
                epoch: 4,
                generation: 7,
                first_slot: 1_000,
                first_turn: TURNS_PER_EVENT as u32,
                turns_in_epoch,
                len: 10,
                leaders: second,
            },
            LeaderScheduleEvent::Leader {
                pid: 1,
                tid: 1,
                timestamp_ns: 0,
                epoch: 4,
                generation: 7,
                index: 1,
                count: 2,
                identity: [2; 32],
            },
            LeaderScheduleEvent::Turns {
                pid: 1,
                tid: 1,
                timestamp_ns: 0,
                epoch: 4,
                generation: 7,
                first_slot: 1_000,
                first_turn: 0,
                turns_in_epoch,
                len: TURNS_PER_EVENT as u16,
                leaders: first,
            },
            LeaderScheduleEvent::Leader {
                pid: 1,
                tid: 1,
                timestamp_ns: 0,
                epoch: 4,
                generation: 7,
                index: 0,
                count: 2,
                identity: [1; 32],
            },
        ];
        let mut set = ScheduleSet::default();
        let completed: Vec<_> = events.into_iter().filter_map(|e| set.apply(e)).collect();
        assert_eq!(completed, vec![4]);
        let info = &set.complete[&4];
        assert_eq!(info.turns.len(), turns_in_epoch as usize);
        assert_eq!(info.turns[1], 1);
        assert_eq!(info.turns[TURNS_PER_EVENT.saturating_add(9)], 1);
        assert_eq!(info.leaders[1], Pubkey::new_from_array([2; 32]).to_string());
        assert_eq!(info.start_slot, 1_000);
        assert_eq!(
            info.slots_in_epoch,
            u64::from(turns_in_epoch).saturating_mul(4)
        );
    }

    fn turns_event(
        epoch: Epoch,
        generation: u64,
        first_turn: u32,
        len: u16,
    ) -> LeaderScheduleEvent {
        LeaderScheduleEvent::Turns {
            pid: 1,
            tid: 1,
            timestamp_ns: 0,
            epoch,
            generation,
            first_slot: 0,
            first_turn,
            turns_in_epoch: 4,
            len,
            leaders: [0; TURNS_PER_EVENT],
        }
    }

    fn leader_event(epoch: Epoch, generation: u64) -> LeaderScheduleEvent {
        LeaderScheduleEvent::Leader {
            pid: 1,
            tid: 1,
            timestamp_ns: 0,
            epoch,
            generation,
            index: 0,
            count: 1,
            identity: [1; 32],
        }
    }

    #[test]
    fn test_schedule_generation_change_restarts_the_set() {
        let mut set = ScheduleSet::default();
        assert_eq!(set.apply(leader_event(4, 1)), None);
        assert_eq!(set.apply(turns_event(4, 1, 0, 2)), None);
        // A new generation discards the half-built one, so its turns alone do not complete it.
        assert_eq!(set.apply(turns_event(4, 2, 2, 2)), None);
        assert_eq!(set.apply(leader_event(4, 2)), None);
        assert_eq!(set.apply(turns_event(4, 2, 0, 2)), Some(4));
        // Resent unchanged, the epoch completes again without reporting a change.
        set.apply(leader_event(4, 3));
        assert_eq!(set.apply(turns_event(4, 3, 0, 4)), None);
        assert_eq!(set.sets_completed, 2);
    }

    #[test]
    fn test_schedule_prunes_partial_sets_older_than_kept_epochs() {
        let mut set = ScheduleSet::default();
        set.apply(leader_event(3, 1));
        for epoch in [4, 5, 6] {
            set.apply(leader_event(epoch, 1));
            set.apply(turns_event(epoch, 1, 0, 4));
        }
        assert_eq!(set.complete.keys().copied().collect::<Vec<_>>(), vec![5, 6]);
        assert!(set.pending.is_empty());
    }

    #[test]
    fn test_throughput_skips_catch_up_windows() {
        let mut throughput = Throughput::default();
        let start = Instant::now();
        throughput.sample(start, SystemTime::now());
        let frozen = |throughput: &mut Throughput, slot: Slot| {
            throughput.add(&BankFrozenEvent {
                pid: 1,
                tid: 1,
                timestamp_ns: 0,
                slot,
                bank_id: slot,
                parent_slot: slot.saturating_sub(1),
                leader: [0; 32],
                blockhash: [0; 32],
                transaction_count: 10,
                non_vote_transaction_count: 10,
                failed_transaction_count: 0,
                transaction_entry_count: 0,
                block_cost: 0,
                block_cost_limit: 0,
                account_cost_limit: 0,
                total_fee: 0,
                priority_fee: 0,
            });
        };
        frozen(&mut throughput, 30);
        assert!(
            throughput
                .sample(start + Duration::from_secs(1), SystemTime::now())
                .is_none()
        );
        frozen(&mut throughput, 33);
        assert!(
            throughput
                .sample(start + Duration::from_secs(2), SystemTime::now())
                .is_some()
        );
    }

    #[test]
    fn test_throughput_rates_per_second() {
        let mut throughput = Throughput::default();
        let start = Instant::now();
        assert!(throughput.sample(start, SystemTime::now()).is_none());
        for slot in 1..=2 {
            throughput.add(&BankFrozenEvent {
                pid: 1,
                tid: 1,
                timestamp_ns: 0,
                slot,
                bank_id: slot,
                parent_slot: slot.saturating_sub(1),
                leader: [0; 32],
                blockhash: [0; 32],
                transaction_count: 100,
                non_vote_transaction_count: 40,
                failed_transaction_count: 10,
                transaction_entry_count: 0,
                block_cost: 0,
                block_cost_limit: 0,
                account_cost_limit: 0,
                total_fee: 0,
                priority_fee: 0,
            });
        }
        let sample = throughput
            .sample(start + Duration::from_secs(1), SystemTime::now())
            .unwrap();
        assert_eq!(sample.slot, 2);
        assert_eq!(
            (
                sample.tps.total,
                sample.tps.vote,
                sample.tps.non_vote_success,
                sample.tps.non_vote_failed
            ),
            (200.0, 120.0, 60.0, 20.0)
        );
    }
}
