//! The validator's start phases and the supermajority wait as events.
//! Start progress goes out on each change, when the stream turns on and every 5 seconds, with the
//! time in the current phase and the time each finished phase took. Phases that end before the
//! event system exists are not seen.
//! The supermajority wait goes out as a set on each check: members share a `generation`, and
//! `index` and `count` mark when it is complete.

use {
    crate::validator::ValidatorStartProgress,
    agave_event_system::{
        EventSystem, StreamConfig, event,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
    solana_clock::Slot,
    solana_gossip::{
        cluster_info::ClusterInfo, contact_info::ContactInfo,
        crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
    },
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_time_utils::timestamp,
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

pub const START_PROGRESS_STREAM: StreamName =
    agave_event_system::stream_name!("node.start_progress");
pub const SUPERMAJORITY_WAIT_STREAM: StreamName =
    agave_event_system::stream_name!("node.supermajority_wait");

/// How many start phases `phase_code` numbers.
pub const PHASES: usize = 11;
/// Bytes of a peer's version text.
pub const VERSION_BYTES: usize = 32;

const POLL: Duration = Duration::from_millis(100);
const RESEND: Duration = Duration::from_secs(5);
const PACE_BATCH: usize = 512;
const PACE_PAUSE: Duration = Duration::from_millis(50);

/// The phase in `ValidatorStartProgress` declaration order.
pub fn phase_code(progress: &ValidatorStartProgress) -> u8 {
    match progress {
        ValidatorStartProgress::Initializing => 0,
        ValidatorStartProgress::SearchingForRpcService => 1,
        ValidatorStartProgress::DownloadingSnapshot { .. } => 2,
        ValidatorStartProgress::CleaningBlockStore => 3,
        ValidatorStartProgress::CleaningAccounts => 4,
        ValidatorStartProgress::LoadingLedger => 5,
        ValidatorStartProgress::ProcessingLedger { .. } => 6,
        ValidatorStartProgress::StartingServices => 7,
        ValidatorStartProgress::Halted => 8,
        ValidatorStartProgress::WaitingForSupermajority { .. } => 9,
        ValidatorStartProgress::Running => 10,
    }
}

/// The current phase and the time each finished one took. The timestamp is monotonic nanoseconds.
/// Snapshot downloads end before the event system exists, so their source is not carried.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartProgressEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// As `phase_code` numbers it.
    pub phase: u8,
    /// Replay's slot and target, or the wait's slot.
    pub slot: Slot,
    pub max_slot: Slot,
    pub gossip_stake_percent: u64,
    /// Time in this round of the phase as of `timestamp_ns`.
    pub phase_elapsed_ns: u64,
    /// The first slot replay reported.
    pub has_replay_origin: bool,
    pub replay_origin: Slot,
    /// Finished phases in the order they first finished, a phase met again summed into one.
    pub taken_len: u8,
    pub taken_phases: [u8; PHASES],
    pub taken_ns: [u64; PHASES],
}

/// Times each phase from when it is first seen.
#[derive(Debug)]
struct PhaseClock {
    current: (u8, Instant),
    taken: Vec<(u8, u64)>,
    replay_origin: Option<Slot>,
    last: ValidatorStartProgress,
}

impl PhaseClock {
    /// `since` is when the phase under way began, which can be before this clock.
    fn starting(progress: ValidatorStartProgress, since: Instant) -> Self {
        let mut clock = Self {
            current: (phase_code(&progress), since),
            taken: Vec::new(),
            replay_origin: None,
            last: progress,
        };
        clock.note_replay(&progress);
        clock
    }

    fn note_replay(&mut self, progress: &ValidatorStartProgress) {
        if let ValidatorStartProgress::ProcessingLedger { slot, .. } = progress {
            self.replay_origin.get_or_insert(*slot);
        }
    }

    /// True where `progress` differs from the last seen.
    fn observe(&mut self, progress: ValidatorStartProgress, now: Instant) -> bool {
        self.note_replay(&progress);
        let code = phase_code(&progress);
        let (held, since) = self.current;
        if held != code {
            let took =
                u64::try_from(now.saturating_duration_since(since).as_nanos()).unwrap_or(u64::MAX);
            match self.taken.iter_mut().find(|(phase, _)| *phase == held) {
                Some((_, total)) => *total = total.saturating_add(took),
                None => self.taken.push((held, took)),
            }
            self.current = (code, now);
        }
        let changed = self.last != progress;
        self.last = progress;
        changed
    }

    fn event(&self, now: Instant) -> StartProgressEvent {
        let (tid, pid) = cached_thread_and_process_id();
        let (slot, max_slot, gossip_stake_percent) = match self.last {
            ValidatorStartProgress::DownloadingSnapshot { slot, .. } => (slot, 0, 0),
            ValidatorStartProgress::ProcessingLedger { slot, max_slot } => (slot, max_slot, 0),
            ValidatorStartProgress::WaitingForSupermajority {
                slot,
                gossip_stake_percent,
            } => (slot, 0, gossip_stake_percent),
            _ => (0, 0, 0),
        };
        let mut taken_phases = [0u8; PHASES];
        let mut taken_ns = [0u64; PHASES];
        for ((phase, took), (phase_slot, ns_slot)) in self
            .taken
            .iter()
            .zip(taken_phases.iter_mut().zip(taken_ns.iter_mut()))
        {
            *phase_slot = *phase;
            *ns_slot = *took;
        }
        StartProgressEvent {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            phase: self.current.0,
            slot,
            max_slot,
            gossip_stake_percent,
            phase_elapsed_ns: u64::try_from(
                now.saturating_duration_since(self.current.1).as_nanos(),
            )
            .unwrap_or(u64::MAX),
            has_replay_origin: self.replay_origin.is_some(),
            replay_origin: self.replay_origin.unwrap_or_default(),
            taken_len: u8::try_from(self.taken.len().min(PHASES)).unwrap_or(u8::MAX),
            taken_phases,
            taken_ns,
        }
    }
}

/// Publishes start progress for the whole run.
pub struct StartProgressEvents {
    stop: Arc<AtomicBool>,
    thread_hdl: Option<JoinHandle<()>>,
}

impl StartProgressEvents {
    /// `phase_since` is when the phase under way began.
    pub fn new(
        event_system: &EventSystem,
        start_progress: Arc<RwLock<ValidatorStartProgress>>,
        phase_since: Instant,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, String> {
        let factory = event_system
            .create_stream::<StartProgressEvent>(
                START_PROGRESS_STREAM,
                StreamConfig {
                    capacity: 64,
                    publisher_slots: 1,
                    subscriber_slots: 8,
                },
            )
            .map_err(|err| format!("Failed to create start progress stream: {err}"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        // Read here, so a phase the validator moves to before the thread runs is timed from then.
        let mut clock = PhaseClock::starting(*start_progress.read().unwrap(), phase_since);
        let thread_hdl = Builder::new()
            .name("solEvtStart".to_string())
            .spawn(move || {
                let mut publisher = factory
                    .try_create_publisher()
                    .expect("fresh start progress stream has one publisher slot");
                let mut was_on = false;
                let mut sent: Option<Instant> = None;
                while !exit.load(Ordering::Relaxed) && !stopped.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    let changed = clock.observe(*start_progress.read().unwrap(), now);
                    let on = publisher.is_enabled();
                    let due = sent.is_none_or(|at| now.duration_since(at) >= RESEND);
                    if on && (changed || !was_on || due) {
                        let _ = publisher.publish(&clock.event(now));
                        sent = Some(now);
                    }
                    was_on = on;
                    thread::sleep(POLL);
                }
            })
            .map_err(|err| format!("Failed to spawn solEvtStart: {err}"))?;
        Ok(Self {
            stop,
            thread_hdl: Some(thread_hdl),
        })
    }

    pub fn join(mut self) -> thread::Result<()> {
        self.thread_hdl.take().map_or(Ok(()), JoinHandle::join)
    }
}

impl Drop for StartProgressEvents {
    /// A validator that fails to start drops this without joining it.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// One staked node the supermajority wait counts. The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupermajorityWaitEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    /// The slot the wait is for.
    pub slot: Slot,
    pub shred_version: u16,
    /// The fields of `wfsm_gossip`, in lamports.
    pub online_stake: u64,
    pub total_activated_stake: u64,
    pub identity: [u8; 32],
    pub activated_stake: u64,
    /// In gossip with a fresh contact, or this node; `version` is empty where not.
    pub seen: bool,
    pub version: [u8; VERSION_BYTES],
}

/// A staked node, with its gossip version where it was seen, which is how it counts as seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitNode {
    pub identity: Pubkey,
    pub activated_stake: u64,
    pub version: Option<String>,
}

/// The nodes the wait counts, as `get_stake_percent_in_gossip` counts them, with the online and
/// total stake.
pub fn wait_nodes(bank: &Bank, cluster_info: &ClusterInfo) -> (Vec<WaitNode>, u64, u64) {
    let now = timestamp();
    let mut versions: HashMap<Pubkey, String> = cluster_info
        .tvu_peers(ContactInfo::clone)
        .into_iter()
        .filter(|node| now.saturating_sub(node.wallclock()) < CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS)
        .map(|node| (*node.pubkey(), node.version().to_string()))
        .collect();
    versions.insert(
        cluster_info.id(),
        cluster_info.my_contact_info().version().to_string(),
    );
    let Some(staked_nodes) = bank.epoch_staked_nodes(bank.epoch()) else {
        return (Vec::new(), 0, 0);
    };
    let (mut online, mut total) = (0u64, 0u64);
    let nodes = staked_nodes
        .iter()
        .map(|(identity, stake)| {
            let version = versions.get(identity).cloned();
            total = total.saturating_add(*stake);
            if version.is_some() {
                online = online.saturating_add(*stake);
            }
            WaitNode {
                identity: *identity,
                activated_stake: *stake,
                version,
            }
        })
        .collect();
    (nodes, online, total)
}

/// One check of the wait as a set, ordered by identity.
pub(crate) fn supermajority_wait_events(
    generation: u64,
    slot: Slot,
    shred_version: u16,
    online_stake: u64,
    total_activated_stake: u64,
    nodes: &[WaitNode],
) -> Vec<SupermajorityWaitEvent> {
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let mut nodes: Vec<&WaitNode> = nodes.iter().collect();
    nodes.sort_by_key(|node| node.identity);
    let count = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
    nodes
        .into_iter()
        .zip(0u32..)
        .map(|(node, index)| {
            let mut version = [0u8; VERSION_BYTES];
            if let Some(text) = &node.version {
                let bytes = text.as_bytes();
                let len = bytes.len().min(VERSION_BYTES);
                version[..len].copy_from_slice(&bytes[..len]);
            }
            SupermajorityWaitEvent {
                pid,
                tid,
                timestamp_ns,
                generation,
                index,
                count,
                slot,
                shred_version,
                online_stake,
                total_activated_stake,
                identity: node.identity.to_bytes(),
                activated_stake: node.activated_stake,
                seen: node.version.is_some(),
                version,
            }
        })
        .collect()
}

/// Publishes the supermajority wait, once per check.
pub struct SupermajorityWaitEvents {
    publisher: Publisher<SupermajorityWaitEvent>,
    generation: u64,
}

impl SupermajorityWaitEvents {
    pub fn new(event_system: &EventSystem) -> Result<Self, String> {
        let publisher = event_system
            .create_stream::<SupermajorityWaitEvent>(
                SUPERMAJORITY_WAIT_STREAM,
                StreamConfig {
                    capacity: 4096,
                    publisher_slots: 1,
                    subscriber_slots: 8,
                },
            )
            .map_err(|err| format!("Failed to create supermajority wait stream: {err}"))?
            .try_create_publisher()
            .ok_or_else(|| "Supermajority wait stream has no publisher slot".to_string())?;
        Ok(Self {
            publisher,
            generation: 0,
        })
    }

    /// Sends one check, paced so a set larger than the stream's capacity arrives whole.
    pub fn publish(&mut self, bank: &Bank, cluster_info: &ClusterInfo, slot: Slot) {
        if !self.publisher.is_enabled() {
            return;
        }
        let (nodes, online, total) = wait_nodes(bank, cluster_info);
        self.generation = self.generation.saturating_add(1);
        let events = supermajority_wait_events(
            self.generation,
            slot,
            cluster_info.my_shred_version(),
            online,
            total,
            &nodes,
        );
        for (batch_index, batch) in events.chunks(PACE_BATCH).enumerate() {
            if batch_index > 0 {
                thread::sleep(PACE_PAUSE);
            }
            for event in batch {
                let _ = self.publisher.publish(event);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn test_a_phase_met_twice_is_one_entry_summed() {
        let start = Instant::now();
        let mut clock = PhaseClock::starting(ValidatorStartProgress::LoadingLedger, start);
        assert!(!clock.observe(ValidatorStartProgress::LoadingLedger, start + secs(1)));
        assert!(clock.observe(ValidatorStartProgress::CleaningBlockStore, start + secs(10)));
        assert!(clock.observe(ValidatorStartProgress::LoadingLedger, start + secs(12)));
        assert!(clock.observe(ValidatorStartProgress::StartingServices, start + secs(18)));
        assert_eq!(
            clock.taken,
            vec![
                (5, secs(16).as_nanos() as u64),
                (3, secs(2).as_nanos() as u64)
            ]
        );
        let event = clock.event(start + secs(20));
        assert_eq!(event.phase, 7);
        assert_eq!(event.phase_elapsed_ns, secs(2).as_nanos() as u64);
        assert_eq!(event.taken_len, 2);
        assert_eq!(&event.taken_phases[..2], &[5, 3]);
        assert_eq!(event.taken_ns[0], secs(16).as_nanos() as u64);
    }

    #[test]
    fn test_the_phase_under_way_is_timed_from_when_it_began() {
        let start = Instant::now();
        let clock = PhaseClock::starting(ValidatorStartProgress::LoadingLedger, start);
        assert_eq!(
            clock.event(start + secs(90)).phase_elapsed_ns,
            secs(90).as_nanos() as u64
        );
    }

    #[test]
    fn test_replay_progress_is_a_change_and_its_first_slot_is_kept() {
        let start = Instant::now();
        let mut clock = PhaseClock::starting(ValidatorStartProgress::LoadingLedger, start);
        let replay = |slot| ValidatorStartProgress::ProcessingLedger {
            slot,
            max_slot: 500,
        };
        assert!(clock.observe(replay(100), start + secs(1)));
        assert!(clock.observe(replay(140), start + secs(2)));
        let event = clock.event(start + secs(3));
        assert!(event.has_replay_origin);
        assert_eq!(event.replay_origin, 100);
        assert_eq!((event.slot, event.max_slot), (140, 500));
        assert_eq!(event.phase_elapsed_ns, secs(2).as_nanos() as u64);
    }

    #[test]
    fn test_the_thread_stops_when_dropped_unjoined() {
        let dir = tempfile::tempdir().unwrap();
        let event_system = EventSystem::new(dir.path()).unwrap();
        let progress = Arc::new(RwLock::new(ValidatorStartProgress::LoadingLedger));
        let events = StartProgressEvents::new(
            &event_system,
            progress,
            Instant::now(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        let stop = events.stop.clone();
        drop(events);
        assert!(stop.load(Ordering::Relaxed));
    }

    #[test]
    fn test_phase_codes_follow_declaration_order() {
        assert_eq!(phase_code(&ValidatorStartProgress::Initializing), 0);
        assert_eq!(phase_code(&ValidatorStartProgress::Running), 10);
        assert_eq!(
            usize::from(phase_code(&ValidatorStartProgress::Running)),
            PHASES - 1
        );
    }

    #[test]
    fn test_a_wait_check_is_one_set_ordered_by_identity() {
        let nodes: Vec<WaitNode> = (0..3)
            .map(|n| WaitNode {
                identity: Pubkey::new_unique(),
                activated_stake: 10 + n,
                version: (n != 1).then(|| "4.5.0-beta.0".to_string()),
            })
            .collect();
        let events = supermajority_wait_events(7, 100, 9, 21, 33, &nodes);
        assert_eq!(events.len(), 3);
        let mut sorted: Vec<_> = nodes.iter().map(|node| node.identity.to_bytes()).collect();
        sorted.sort();
        for (event, (index, identity)) in events.iter().zip(sorted.iter().enumerate()) {
            assert_eq!(event.generation, 7);
            assert_eq!(event.count, 3);
            assert_eq!(event.index as usize, index);
            assert_eq!(&event.identity, identity);
            assert_eq!((event.online_stake, event.total_activated_stake), (21, 33));
            assert_eq!((event.slot, event.shred_version), (100, 9));
        }
        let unseen = events
            .iter()
            .find(|event| event.activated_stake == 11)
            .unwrap();
        assert!(!unseen.seen);
        assert_eq!(unseen.version, [0u8; VERSION_BYTES]);
        let seen = events
            .iter()
            .find(|event| event.activated_stake == 10)
            .unwrap();
        assert!(seen.seen);
        assert_eq!(&seen.version[..12], b"4.5.0-beta.0");
        assert_eq!(seen.version[12], 0);
    }
}
