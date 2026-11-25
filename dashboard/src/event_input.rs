//! Dashboard input read from the validator's event streams by a dashboard in its own process.
//! Test branch only: the streams are this branch's, not Anza's.

use {
    crate::{
        collect::EpochInfo,
        events::{
            ADMITTED_STREAM, AdmittedEvent, BLOCK_STREAM, BlockEvent, EPOCH_STREAM, EpochEvent,
            FINALIZATION_STREAM, FinalizationEvent, GOSSIP_PEER_STREAM, GossipPeerEvent,
            LEDGER_SLOT_STREAM, LedgerSlotEvent, MESSAGE_VERSIONS_STREAM, MessageVersionsEvent,
            NODE_INFO_STREAM, NodeInfoEvent, OWN_BLOCK_STREAM, OwnBlockEvent, SLOT_CLOCK_STREAM,
            SLOT_HISTORY_STREAM, SLOT_STATUS_STREAM, SlotClockEvent, SlotHistoryEvent,
            SlotStatusEvent, TX_OUTCOME_STREAM, TxOutcomeEvent, VALIDATOR_INFO_STREAM,
            ValidatorInfoEvent,
        },
        metric_events::{
            METRIC_ACCOUNTS_STREAM, METRIC_GOSSIP_STREAM, METRIC_LEADER_STREAM, METRIC_NET_STREAM,
            METRIC_ONCE_STREAM, METRIC_SLOT_STREAM, METRIC_TPU_STREAM, METRIC_VOTOR_STREAM,
            MetricAccounts, MetricEvent, MetricGossip, MetricLeader, MetricNet, MetricOnce,
            MetricSlot, MetricTpu, MetricVotor,
        },
        metrics_tap::MetricsTap,
        mirror::{Mirror, SetStream},
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
        start_events::{
            START_PROGRESS_STREAM, SUPERMAJORITY_WAIT_STREAM, StartProgressEvent,
            SupermajorityWaitEvent,
        },
    },
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, HashMap},
        ffi::OsString,
        fmt::Write as _,
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, Instant},
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

macro_rules! stamped {
    ($($event:ty),*) => {$(
        impl Stamped for $event {
            fn timestamp_ns(&self) -> u64 {
                self.timestamp_ns
            }
        }
    )*};
}

stamped!(
    NodeInfoEvent,
    StartProgressEvent,
    SupermajorityWaitEvent,
    SlotStatusEvent,
    EpochEvent,
    SlotClockEvent,
    LedgerSlotEvent,
    BlockEvent,
    OwnBlockEvent,
    AdmittedEvent,
    SlotHistoryEvent,
    TxOutcomeEvent,
    MessageVersionsEvent,
    GossipPeerEvent,
    ValidatorInfoEvent,
    FinalizationEvent
);

macro_rules! stamped_metric {
    ($($event:ty),*) => {$(
        impl Stamped for $event {
            fn timestamp_ns(&self) -> u64 {
                MetricEvent::timestamp_ns(self)
            }
        }
    )*};
}

stamped_metric!(
    MetricSlot,
    MetricLeader,
    MetricVotor,
    MetricTpu,
    MetricNet,
    MetricAccounts,
    MetricGossip,
    MetricOnce
);

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
        // Keep the previous, this and the next epoch, and no partial set older than them.
        while self.complete.len() > 3 {
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

/// A metric event as its datapoint, for one closure over every row.
trait MetricPoint {
    fn point(&self) -> (solana_metrics::datapoint::DataPoint, u64);
}

impl<E: MetricEvent> MetricPoint for E {
    fn point(&self) -> (solana_metrics::datapoint::DataPoint, u64) {
        self.to_point()
    }
}

/// Every subscription, feeding the mirror.
pub struct EventInput {
    dir: PathBuf,
    checked: Option<Instant>,
    started: Instant,
    mirror: Arc<Mirror>,
    tap: Arc<MetricsTap>,
    pub slot_events: Stream<SlotEvent>,
    pub bank_frozen: Stream<BankFrozenEvent>,
    pub shred_fetch: Stream<ShredFetchCounters>,
    pub vote_accounts: Stream<VoteAccountEvent>,
    pub schedule: Stream<LeaderScheduleEvent>,
    pub node: Stream<NodeInfoEvent>,
    pub progress: Stream<StartProgressEvent>,
    pub wait: Stream<SupermajorityWaitEvent>,
    pub status: Stream<SlotStatusEvent>,
    pub epoch: Stream<EpochEvent>,
    pub clock: Stream<SlotClockEvent>,
    pub ledger: Stream<LedgerSlotEvent>,
    pub blocks: Stream<BlockEvent>,
    pub own_blocks: Stream<OwnBlockEvent>,
    pub admitted: Stream<AdmittedEvent>,
    pub history: Stream<SlotHistoryEvent>,
    pub outcomes: Stream<TxOutcomeEvent>,
    pub versions: Stream<MessageVersionsEvent>,
    pub peers: Stream<GossipPeerEvent>,
    pub info: Stream<ValidatorInfoEvent>,
    pub finalization: Stream<FinalizationEvent>,
    pub metric_slot: Stream<MetricSlot>,
    pub metric_leader: Stream<MetricLeader>,
    pub metric_votor: Stream<MetricVotor>,
    pub metric_tpu: Stream<MetricTpu>,
    pub metric_net: Stream<MetricNet>,
    pub metric_accounts: Stream<MetricAccounts>,
    pub metric_gossip: Stream<MetricGossip>,
    pub metric_once: Stream<MetricOnce>,
    pub slots: SlotProgress,
    /// Newest totals per path, turbine first.
    pub shred_totals: [Option<ShredFetchCounters>; 2],
    pub votes: VoteSet,
    pub schedules: ScheduleSet,
    pub first_vote_set: Option<Duration>,
    pub first_schedule: Option<Duration>,
}

impl EventInput {
    pub fn new(dir: PathBuf, mirror: Arc<Mirror>, tap: Arc<MetricsTap>) -> Self {
        Self {
            dir,
            checked: None,
            started: Instant::now(),
            mirror,
            tap,
            slot_events: Stream::new(SLOT_EVENT_STREAM),
            bank_frozen: Stream::new(BANK_FROZEN_STREAM),
            shred_fetch: Stream::new(SHRED_FETCH_STREAM),
            vote_accounts: Stream::new(VOTE_ACCOUNT_STREAM),
            schedule: Stream::new(LEADER_SCHEDULE_STREAM),
            node: Stream::new(NODE_INFO_STREAM),
            progress: Stream::new(START_PROGRESS_STREAM),
            wait: Stream::new(SUPERMAJORITY_WAIT_STREAM),
            status: Stream::new(SLOT_STATUS_STREAM),
            epoch: Stream::new(EPOCH_STREAM),
            clock: Stream::new(SLOT_CLOCK_STREAM),
            ledger: Stream::new(LEDGER_SLOT_STREAM),
            blocks: Stream::new(BLOCK_STREAM),
            own_blocks: Stream::new(OWN_BLOCK_STREAM),
            admitted: Stream::new(ADMITTED_STREAM),
            history: Stream::new(SLOT_HISTORY_STREAM),
            outcomes: Stream::new(TX_OUTCOME_STREAM),
            versions: Stream::new(MESSAGE_VERSIONS_STREAM),
            peers: Stream::new(GOSSIP_PEER_STREAM),
            info: Stream::new(VALIDATOR_INFO_STREAM),
            finalization: Stream::new(FINALIZATION_STREAM),
            metric_slot: Stream::new(METRIC_SLOT_STREAM),
            metric_leader: Stream::new(METRIC_LEADER_STREAM),
            metric_votor: Stream::new(METRIC_VOTOR_STREAM),
            metric_tpu: Stream::new(METRIC_TPU_STREAM),
            metric_net: Stream::new(METRIC_NET_STREAM),
            metric_accounts: Stream::new(METRIC_ACCOUNTS_STREAM),
            metric_gossip: Stream::new(METRIC_GOSSIP_STREAM),
            metric_once: Stream::new(METRIC_ONCE_STREAM),
            slots: SlotProgress::default(),
            shred_totals: [None, None],
            votes: VoteSet::default(),
            schedules: ScheduleSet::default(),
            first_vote_set: None,
            first_schedule: None,
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
        self.node.connect(dir, &explorer);
        self.progress.connect(dir, &explorer);
        self.status.connect(dir, &explorer);
        self.epoch.connect(dir, &explorer);
        self.clock.connect(dir, &explorer);
        self.ledger.connect(dir, &explorer);
        self.blocks.connect(dir, &explorer);
        let own_resubscribed = self.own_blocks.connect(dir, &explorer);
        if own_resubscribed {
            self.mirror.reset_set(SetStream::OwnBlocks);
        }
        self.mirror
            .set_own_blocks_connected(self.own_blocks.connected(), own_resubscribed);
        self.versions.connect(dir, &explorer);
        for (resubscribed, stream) in [
            (self.wait.connect(dir, &explorer), SetStream::Wait),
            (self.admitted.connect(dir, &explorer), SetStream::Admitted),
            (self.history.connect(dir, &explorer), SetStream::History),
            (self.outcomes.connect(dir, &explorer), SetStream::Outcomes),
            (self.peers.connect(dir, &explorer), SetStream::Peers),
            (self.info.connect(dir, &explorer), SetStream::Info),
        ] {
            if resubscribed {
                self.mirror.reset_set(stream);
            }
        }
        // A new queue, or none, means no reading is owed for the banks in flight.
        if self.finalization.connect(dir, &explorer) || !self.finalization.connected() {
            self.mirror.reset_finalization();
        }
        self.metric_slot.connect(dir, &explorer);
        self.metric_leader.connect(dir, &explorer);
        self.metric_votor.connect(dir, &explorer);
        self.metric_tpu.connect(dir, &explorer);
        self.metric_net.connect(dir, &explorer);
        self.metric_accounts.connect(dir, &explorer);
        self.metric_gossip.connect(dir, &explorer);
        self.metric_once.connect(dir, &explorer);
    }

    pub fn disconnect(&mut self) {
        self.slot_events.disconnect();
        self.bank_frozen.disconnect();
        self.shred_fetch.disconnect();
        self.vote_accounts.disconnect();
        self.schedule.disconnect();
        self.node.disconnect();
        self.progress.disconnect();
        self.wait.disconnect();
        self.status.disconnect();
        self.epoch.disconnect();
        self.clock.disconnect();
        self.ledger.disconnect();
        self.blocks.disconnect();
        self.own_blocks.disconnect();
        self.mirror.set_own_blocks_connected(false, false);
        self.admitted.disconnect();
        self.history.disconnect();
        self.outcomes.disconnect();
        self.versions.disconnect();
        self.peers.disconnect();
        self.info.disconnect();
        self.finalization.disconnect();
        self.metric_slot.disconnect();
        self.metric_leader.disconnect();
        self.metric_votor.disconnect();
        self.metric_tpu.disconnect();
        self.metric_net.disconnect();
        self.metric_accounts.disconnect();
        self.metric_gossip.disconnect();
        self.metric_once.disconnect();
    }

    pub fn drain(&mut self) {
        let slots = &mut self.slots;
        let mut rooted = Vec::new();
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
            SlotEvent::Rooted { slot, .. } => {
                slots.root = slots.root.max(slot);
                rooted.push(slot);
            }
        });
        for slot in rooted {
            self.mirror.apply_rooted(slot);
        }

        let mirror = &self.mirror;
        self.bank_frozen.drain(|event| mirror.apply_frozen(&event));
        self.ledger.drain(|event| mirror.apply_ledger(&event));
        self.blocks.drain(|event| mirror.apply_block(&event));
        self.own_blocks
            .drain(|event| mirror.apply_own_block(&event));
        self.admitted.drain(|event| mirror.apply_admitted(&event));
        self.history.drain(|event| mirror.apply_history(&event));
        // Versions first: a block reads as full once its outcomes are applied.
        self.versions.drain(|event| mirror.apply_versions(&event));
        self.outcomes.drain(|event| mirror.apply_outcome(&event));
        self.peers.drain(|event| mirror.apply_peer(&event));
        self.info.drain(|event| mirror.apply_info(&event));
        self.finalization
            .drain(|event| mirror.apply_finalization(&event));

        let shred_totals = &mut self.shred_totals;
        self.shred_fetch.drain(|event| {
            let path = usize::from(event.path == SHRED_FETCH_PATH_REPAIR);
            shred_totals[path] = Some(event);
        });

        let mirror = &self.mirror;
        self.node.drain(|event| mirror.apply_node(&event));
        self.progress.drain(|event| mirror.apply_progress(&event));
        self.wait.drain(|event| mirror.apply_wait(&event));
        self.status.drain(|event| mirror.apply_status(&event));
        self.epoch.drain(|event| mirror.apply_epoch(&event));
        self.clock.drain(|event| mirror.apply_clock(&event));

        let tap = &self.tap;
        let observe = |event: &dyn MetricPoint| {
            let (point, observed_ms) = event.point();
            tap.observe_external(&point, observed_ms);
        };
        self.metric_slot.drain(|event| observe(&event));
        self.metric_leader.drain(|event| observe(&event));
        self.metric_votor.drain(|event| observe(&event));
        self.metric_tpu.drain(|event| observe(&event));
        self.metric_net.drain(|event| observe(&event));
        self.metric_accounts.drain(|event| observe(&event));
        self.metric_gossip.drain(|event| observe(&event));
        self.metric_once.drain(|event| observe(&event));

        let votes = &mut self.votes;
        let mut votes_changed = false;
        self.vote_accounts
            .drain(|event| votes_changed |= votes.apply(event));
        if votes_changed {
            if self.first_vote_set.is_none() {
                self.first_vote_set = Some(self.started.elapsed());
            }
            if let Some(accounts) = self.votes.accounts() {
                self.mirror.apply_votes(accounts);
            }
        }

        let schedules = &mut self.schedules;
        let mut completed = Vec::new();
        self.schedule
            .drain(|event| completed.extend(schedules.apply(event)));
        if !completed.is_empty() && self.first_schedule.is_none() {
            self.first_schedule = Some(self.started.elapsed());
        }
        for epoch in completed {
            if let Some(info) = self.schedules.complete.get(&epoch) {
                self.mirror.apply_schedule(info);
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
        macro_rules! lines {
            ($($stream:ident),*) => {$(
                line(
                    self.$stream.name(),
                    &self.$stream.stats,
                    self.$stream.connected(),
                    self.$stream.rejected(),
                );
            )*};
        }
        lines!(
            slot_events,
            bank_frozen,
            shred_fetch,
            vote_accounts,
            schedule,
            node,
            progress,
            wait,
            status,
            epoch,
            clock,
            ledger,
            blocks,
            own_blocks,
            admitted,
            history,
            outcomes,
            versions,
            peers,
            info,
            finalization,
            metric_slot,
            metric_leader,
            metric_votor,
            metric_tpu,
            metric_net,
            metric_accounts,
            metric_gossip,
            metric_once
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
             {}, epochs {:?}, first after {:?}; mirror ready {}",
            self.votes.sets_completed,
            self.votes.sets_abandoned,
            self.first_vote_set,
            self.schedules.sets_completed,
            self.schedules.complete.keys().collect::<Vec<_>>(),
            self.first_schedule,
            self.mirror.ready(),
        );
        out
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;

    /// A staked vote account as a full set's only member.
    pub(crate) fn vote_account(key: u8) -> VoteAccountEvent {
        VoteAccountEvent {
            pid: 1,
            tid: 1,
            timestamp_ns: 0,
            slot: 1_000,
            epoch: 3,
            generation: 1,
            index: 0,
            count: 1,
            reason: solana_core::bank_state_events::VOTE_ACCOUNT_FULL_SET,
            vote_account: [key; 32],
            identity: [key; 32],
            activated_stake: 1,
            commission: 0,
            inflation_rewards_commission_bps: 0,
            has_credits: false,
            credits_epoch: 0,
            credits: 0,
            prev_credits: 0,
            has_bls_key: false,
            has_last_voted_slot: true,
            last_voted_slot: 990,
            pending_delegator_rewards: 0,
        }
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
            inflation_rewards_commission_bps: 0,
            has_credits: false,
            credits_epoch: 0,
            credits: 0,
            prev_credits: 0,
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
        assert_eq!(
            set.complete.keys().copied().collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert!(set.pending.is_empty());
    }
}
