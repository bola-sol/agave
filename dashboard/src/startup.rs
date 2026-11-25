//! Publishing the validator's boot phase. Shared by the boot thread and the
//! collector, so the handover between them is invisible to the client.

use {
    crate::{
        metrics_tap::StakeInGossip,
        proto::{Debounced, Publisher, TOPIC_SUMMARY},
        validator_info::ValidatorInfoCache,
    },
    crossbeam_channel::Receiver,
    serde::Serialize,
    solana_clock::Slot,
    solana_core::validator::{GossipReady, ValidatorStartProgress},
    solana_gossip::{
        cluster_info::ClusterInfo, contact_info::ContactInfo,
        crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
    },
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_time_utils::timestamp,
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

pub const KEY_STARTUP_PROGRESS: &str = "startup_progress";
pub const KEY_GOSSIP_STAKE: &str = "gossip_stake";

pub type GossipReadyReceiver = Receiver<GossipReady>;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipStake {
    pub slot: Slot,
    pub shred_version: u16,
    pub total: u64,
    pub seen: u64,
    pub validators: Vec<GossipValidator>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipValidator {
    pub identity: String,
    pub name: Option<String>,
    pub version: Option<String>,
    pub stake: u64,
    pub seen: bool,
}

/// Seen is a TVU peer with a fresh contact, as the validator's own wait counts it, and this node
/// counts as seen.
pub fn gossip_stake(
    cluster_info: &ClusterInfo,
    bank: &Bank,
    names: &ValidatorInfoCache,
) -> GossipStake {
    let shred_version = cluster_info.my_shred_version();
    let now = timestamp();
    let mut contacts: HashMap<Pubkey, String> = cluster_info
        .tvu_peers(ContactInfo::clone)
        .into_iter()
        .filter(|contact| {
            now.saturating_sub(contact.wallclock()) < CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS
        })
        .map(|contact| (*contact.pubkey(), contact.version().to_string()))
        .collect();
    contacts.insert(
        cluster_info.id(),
        cluster_info.my_contact_info().version().to_string(),
    );

    let mut validators: Vec<GossipValidator> = crate::collect::tally_bank_stake(bank)
        .staked
        .into_iter()
        .map(|(identity, stake)| {
            let info = names.get(&identity);
            let version = contacts.get(&identity).cloned();
            GossipValidator {
                identity: identity.to_string(),
                name: info.and_then(|info| info.name.clone()),
                seen: version.is_some(),
                version,
                stake,
            }
        })
        .collect();
    validators.sort_by(|a, b| {
        b.stake
            .cmp(&a.stake)
            .then_with(|| a.identity.cmp(&b.identity))
    });
    let total = validators
        .iter()
        .fold(0, |sum, v| u64::saturating_add(sum, v.stake));
    let seen = validators
        .iter()
        .filter(|v| v.seen)
        .fold(0, |sum, v| u64::saturating_add(sum, v.stake));
    GossipStake {
        slot: bank.slot(),
        shred_version,
        total,
        seen,
        validators,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StartupProgress {
    pub phase: String,
    pub detail: Option<String>,
    pub running: bool,
    /// From 0 to 1, measured from where replay began.
    pub fraction: Option<f64>,
    /// From 0 to 1, a whole percent truncated by the validator.
    pub stake_percent: Option<f64>,
    pub stake_in_gossip: Option<StakeInGossip>,
    /// Since most phases cannot say how far along they are.
    pub phase_elapsed_nanos: u64,
    pub phases_taken: Vec<PhaseTiming>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PhaseTiming {
    pub phase: String,
    pub elapsed_nanos: u64,
}

pub struct StartupPublisher {
    debounce: Debounced<StartupProgress>,
    gossip: Debounced<Option<GossipStake>>,
    /// Replay starts from a snapshot, so `slot / max_slot` would sit near 100% throughout.
    replay_origin: Option<Slot>,
    /// The validator reports its phase but not when it got there.
    current: Option<(String, Instant)>,
    /// Accumulated if a phase comes round again, as `loading_ledger` does.
    taken: Vec<PhaseTiming>,
    /// Whether the current phase was seen to begin; a phase first seen part way is not timed.
    witnessed: bool,
    /// What the validator reported of its start, where this process reads it from events.
    reported: Option<SharedReported>,
}

/// The validator's own phase timings, from its start progress events.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportedTimings {
    pub phase: &'static str,
    pub phase_elapsed_nanos: u64,
    /// Monotonic nanoseconds `phase_elapsed_nanos` was read at.
    pub at_ns: u64,
    pub taken: Vec<PhaseTiming>,
    pub replay_origin: Option<Slot>,
}

/// The validator's start as its events report it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Reported {
    pub timings: Option<ReportedTimings>,
    /// The supermajority wait's newest check.
    pub gossip: Option<GossipStake>,
    /// Whether any reading of the validator's phase has arrived; before one, the phase shown is
    /// only the default.
    pub readings: bool,
}

pub type SharedReported = Arc<RwLock<Reported>>;

/// Phase names in the order `solana_core::start_events::phase_code` numbers them.
const PHASE_NAMES: [&str; solana_core::start_events::PHASES] = [
    "initializing",
    "searching_for_rpc_service",
    "downloading_snapshot",
    "cleaning_blockstore",
    "cleaning_accounts",
    "loading_ledger",
    "processing_ledger",
    "starting_services",
    "halted",
    "waiting_for_supermajority",
    "running",
];

pub fn phase_name(code: u8) -> Option<&'static str> {
    PHASE_NAMES.get(usize::from(code)).copied()
}

impl Default for StartupPublisher {
    fn default() -> Self {
        Self {
            debounce: Debounced::new(TOPIC_SUMMARY, KEY_STARTUP_PROGRESS),
            gossip: Debounced::new(TOPIC_SUMMARY, KEY_GOSSIP_STAKE),
            replay_origin: None,
            current: None,
            taken: Vec::new(),
            witnessed: false,
            reported: None,
        }
    }
}

impl StartupPublisher {
    pub fn publish_gossip(&mut self, publisher: &Publisher, stake: Option<GossipStake>) {
        self.gossip.publish(publisher, stake);
    }

    /// Publishes the validator's reported timings over this process's, which began later.
    pub fn report_from(&mut self, reported: SharedReported) {
        self.reported = Some(reported);
    }

    /// The supermajority wait's newest check, as the validator reported it.
    pub fn reported_gossip(&self) -> Option<GossipStake> {
        self.reported
            .as_ref()
            .and_then(|reported| reported.read().unwrap().gossip.clone())
    }

    pub fn publish(
        &mut self,
        publisher: &Publisher,
        progress: ValidatorStartProgress,
        stake_in_gossip: Option<StakeInGossip>,
    ) {
        self.publish_at(
            publisher,
            progress,
            stake_in_gossip,
            agave_event_system::event_metadata::monotonic_timestamp_ns(),
        );
    }

    /// `now_ns` is monotonic, as the validator's event timestamps are.
    fn publish_at(
        &mut self,
        publisher: &Publisher,
        progress: ValidatorStartProgress,
        stake_in_gossip: Option<StakeInGossip>,
        now_ns: u64,
    ) {
        let reported = self
            .reported
            .as_ref()
            .map(|reported| reported.read().unwrap().clone())
            .unwrap_or_default();
        if let Some(origin) = reported.timings.as_ref().and_then(|t| t.replay_origin) {
            self.replay_origin = Some(origin);
        }
        // During the wait the tap's `wfsm_gossip` comes every tenth check, if at all.
        let stake_in_gossip = stake_in_gossip.or_else(|| {
            reported.gossip.as_ref().map(|gossip| StakeInGossip {
                online: gossip.seen,
                total: gossip.total,
            })
        });
        let phase = describe(progress);
        let waiting = matches!(
            progress,
            ValidatorStartProgress::WaitingForSupermajority { .. }
        );
        let progress = StartupProgress {
            phase: phase.name.to_string(),
            detail: phase.detail,
            running: matches!(progress, ValidatorStartProgress::Running),
            fraction: self.fraction(phase.replay_slots),
            stake_percent: phase.stake_percent,
            stake_in_gossip: stake_in_gossip.filter(|_| waiting),
            phase_elapsed_nanos: if self.reported.is_none() || reported.readings {
                self.elapsed(phase.name, Instant::now())
            } else {
                0
            },
            phases_taken: self.taken.clone(),
        };
        let progress = match reported.timings {
            Some(timings) => StartupProgress {
                phase_elapsed_nanos: if timings.phase == phase.name {
                    let elapsed = timings
                        .phase_elapsed_nanos
                        .saturating_add(now_ns.saturating_sub(timings.at_ns));
                    Duration::from_secs(Duration::from_nanos(elapsed).as_secs()).as_nanos() as u64
                } else {
                    progress.phase_elapsed_nanos
                },
                phases_taken: merged_taken(&progress.phases_taken, timings.taken),
                ..progress
            },
            None => progress,
        };
        self.debounce.publish(publisher, progress);
    }

    /// Whole seconds, so the debounce does not send four messages a second.
    fn elapsed(&mut self, phase: &str, now: Instant) -> u64 {
        match &mut self.current {
            Some((current, since)) if current == phase => whole_seconds(now, *since),
            other => {
                let first = other.is_none();
                if let Some((finished, since)) = other.take().filter(|_| self.witnessed) {
                    let elapsed_nanos = now.duration_since(since).as_nanos() as u64;
                    match self
                        .taken
                        .iter_mut()
                        .find(|timing| timing.phase == finished)
                    {
                        Some(timing) => {
                            timing.elapsed_nanos =
                                timing.elapsed_nanos.saturating_add(elapsed_nanos)
                        }
                        None => self.taken.push(PhaseTiming {
                            phase: finished,
                            elapsed_nanos,
                        }),
                    }
                }
                // Read from another process, the first phase seen began unseen unless the validator
                // was only starting; inside the validator every phase is seen from its start.
                self.witnessed = !first || phase == "initializing" || self.reported.is_none();
                self.current = Some((phase.to_string(), now));
                0
            }
        }
    }

    fn fraction(&mut self, slots: Option<(Slot, Slot)>) -> Option<f64> {
        let (current, target) = slots?;
        let origin = *self.replay_origin.get_or_insert(current);
        // Replay can be handed a target it has already passed, and the origin is
        // only an estimate; neither may run the meter backwards or past the end.
        let span = target.checked_sub(origin)?;
        if span == 0 {
            return Some(1.0);
        }
        let done = current.saturating_sub(origin).min(span);
        Some(done as f64 / span as f64)
    }
}

struct Phase {
    name: &'static str,
    detail: Option<String>,
    replay_slots: Option<(Slot, Slot)>,
    stake_percent: Option<f64>,
}

fn describe(progress: ValidatorStartProgress) -> Phase {
    let (name, detail, replay_slots, stake_percent) = match progress {
        ValidatorStartProgress::Initializing => ("initializing", None, None, None),
        ValidatorStartProgress::SearchingForRpcService => {
            ("searching_for_rpc_service", None, None, None)
        }
        ValidatorStartProgress::DownloadingSnapshot { slot, rpc_addr } => (
            "downloading_snapshot",
            Some(format!("slot {slot} from {rpc_addr}")),
            None,
            None,
        ),
        ValidatorStartProgress::CleaningBlockStore => ("cleaning_blockstore", None, None, None),
        ValidatorStartProgress::CleaningAccounts => ("cleaning_accounts", None, None, None),
        ValidatorStartProgress::LoadingLedger => ("loading_ledger", None, None, None),
        ValidatorStartProgress::ProcessingLedger { slot, max_slot } => (
            "processing_ledger",
            Some(format!("slot {slot} of {max_slot}")),
            Some((slot, max_slot)),
            None,
        ),
        ValidatorStartProgress::StartingServices => ("starting_services", None, None, None),
        ValidatorStartProgress::Halted => ("halted", None, None, None),
        ValidatorStartProgress::WaitingForSupermajority {
            slot,
            gossip_stake_percent,
        } => (
            "waiting_for_supermajority",
            Some(format!("slot {slot}")),
            None,
            Some(gossip_stake_percent as f64 / 100.0),
        ),
        ValidatorStartProgress::Running => ("running", None, None, None),
    };
    Phase {
        name,
        detail,
        replay_slots,
        stake_percent,
    }
}

/// The phases this process timed that the validator did not report, which came before its event
/// system, then the validator's own.
fn merged_taken(seen: &[PhaseTiming], reported: Vec<PhaseTiming>) -> Vec<PhaseTiming> {
    let mut merged: Vec<PhaseTiming> = seen
        .iter()
        .filter(|timing| reported.iter().all(|other| other.phase != timing.phase))
        .cloned()
        .collect();
    merged.extend(reported);
    merged
}

fn whole_seconds(now: Instant, since: Instant) -> u64 {
    Duration::from_secs(now.duration_since(since).as_secs()).as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replaying(slot: Slot, max_slot: Slot) -> ValidatorStartProgress {
        ValidatorStartProgress::ProcessingLedger { slot, max_slot }
    }

    fn published_progress(publisher: &Publisher) -> serde_json::Value {
        let message = publisher
            .snapshot()
            .iter()
            .map(|message| message.to_string())
            .find(|message| message.contains(r#""key":"startup_progress""#))
            .expect("startup progress published");
        serde_json::from_str::<serde_json::Value>(&message).unwrap()["value"].clone()
    }

    fn reported(phase: &'static str, elapsed_secs: u64, at_ns: u64) -> SharedReported {
        Arc::new(RwLock::new(Reported {
            timings: Some(ReportedTimings {
                phase,
                phase_elapsed_nanos: Duration::from_secs(elapsed_secs).as_nanos() as u64,
                at_ns,
                taken: vec![PhaseTiming {
                    phase: "loading_ledger".to_string(),
                    elapsed_nanos: 90,
                }],
                replay_origin: Some(1000),
            }),
            gossip: None,
            readings: true,
        }))
    }

    #[test]
    fn test_a_phase_first_seen_part_way_is_not_timed() {
        let mut publisher = StartupPublisher::default();
        publisher.report_from(SharedReported::default());
        let base = Instant::now();
        publisher.elapsed("processing_ledger", base);
        publisher.elapsed("starting_services", base + Duration::from_secs(5));
        publisher.elapsed("running", base + Duration::from_secs(7));
        let taken: Vec<_> = publisher.taken.iter().map(|t| t.phase.as_str()).collect();
        assert_eq!(taken, ["starting_services"]);
    }

    #[test]
    fn test_no_phase_is_timed_before_the_first_reading() {
        let publisher = Publisher::new();
        let mut startup = StartupPublisher::default();
        let shared = SharedReported::default();
        startup.report_from(shared.clone());
        startup.publish_at(&publisher, ValidatorStartProgress::Initializing, None, 0);
        shared.write().unwrap().readings = true;
        startup.publish_at(&publisher, ValidatorStartProgress::Running, None, 0);
        assert_eq!(
            published_progress(&publisher)["phases_taken"],
            serde_json::json!([])
        );
    }

    #[test]
    fn test_every_phase_code_names_the_phase_it_describes() {
        use solana_core::start_events::phase_code;
        let all = [
            ValidatorStartProgress::Initializing,
            ValidatorStartProgress::SearchingForRpcService,
            ValidatorStartProgress::DownloadingSnapshot {
                slot: 1,
                rpc_addr: "1.2.3.4:8899".parse().unwrap(),
            },
            ValidatorStartProgress::CleaningBlockStore,
            ValidatorStartProgress::CleaningAccounts,
            ValidatorStartProgress::LoadingLedger,
            replaying(1, 2),
            ValidatorStartProgress::StartingServices,
            ValidatorStartProgress::Halted,
            ValidatorStartProgress::WaitingForSupermajority {
                slot: 1,
                gossip_stake_percent: 5,
            },
            ValidatorStartProgress::Running,
        ];
        for progress in all {
            assert_eq!(
                phase_name(phase_code(&progress)),
                Some(describe(progress).name)
            );
        }
    }

    #[test]
    fn test_reported_timings_replace_this_processs_and_run_on() {
        let publisher = Publisher::new();
        let mut startup = StartupPublisher::default();
        startup.report_from(reported("processing_ledger", 10, 1_000));
        let five_and_a_half = Duration::from_millis(5_500).as_nanos() as u64;
        startup.publish_at(
            &publisher,
            replaying(1500, 2000),
            None,
            1_000 + five_and_a_half,
        );
        let value = published_progress(&publisher);
        assert_eq!(value["phase_elapsed_nanos"], 15_000_000_000u64);
        assert_eq!(value["fraction"], 0.5);
        assert_eq!(value["phases_taken"][0]["phase"], "loading_ledger");
        assert_eq!(value["phases_taken"][0]["elapsed_nanos"], 90);
    }

    #[test]
    fn test_reported_timings_for_another_phase_leave_the_local_clock() {
        let publisher = Publisher::new();
        let mut startup = StartupPublisher::default();
        startup.report_from(reported("loading_ledger", 10, 0));
        startup.publish_at(
            &publisher,
            ValidatorStartProgress::StartingServices,
            None,
            0,
        );
        assert_eq!(published_progress(&publisher)["phase_elapsed_nanos"], 0);
    }

    #[test]
    fn test_phases_seen_before_the_reports_come_first() {
        let seen = vec![
            PhaseTiming {
                phase: "searching_for_rpc_service".to_string(),
                elapsed_nanos: 18,
            },
            PhaseTiming {
                phase: "loading_ledger".to_string(),
                elapsed_nanos: 3,
            },
        ];
        let reported = vec![PhaseTiming {
            phase: "loading_ledger".to_string(),
            elapsed_nanos: 90,
        }];
        let merged = merged_taken(&seen, reported);
        assert_eq!(
            merged
                .iter()
                .map(|timing| (timing.phase.as_str(), timing.elapsed_nanos))
                .collect::<Vec<_>>(),
            [("searching_for_rpc_service", 18), ("loading_ledger", 90)]
        );
    }

    #[test]
    fn test_the_reported_wait_gives_the_stake_in_gossip() {
        let publisher = Publisher::new();
        let mut startup = StartupPublisher::default();
        let shared = SharedReported::default();
        shared.write().unwrap().gossip = Some(GossipStake {
            slot: 7,
            shred_version: 1,
            total: 100,
            seen: 70,
            validators: Vec::new(),
        });
        startup.report_from(shared);
        let waiting = ValidatorStartProgress::WaitingForSupermajority {
            slot: 7,
            gossip_stake_percent: 70,
        };
        startup.publish_at(&publisher, waiting, None, 0);
        let value = published_progress(&publisher);
        assert_eq!(value["stake_in_gossip"]["online"], 70);
        assert_eq!(value["stake_in_gossip"]["total"], 100);
        assert_eq!(
            startup.reported_gossip().map(|gossip| gossip.seen),
            Some(70)
        );
    }

    #[test]
    fn test_fraction_is_measured_from_the_first_slot_seen() {
        let mut publisher = StartupPublisher::default();
        assert_eq!(publisher.fraction(Some((1000, 2000))), Some(0.0));
        assert_eq!(publisher.fraction(Some((1500, 2000))), Some(0.5));
        assert_eq!(publisher.fraction(Some((2000, 2000))), Some(1.0));
    }

    #[test]
    fn test_fraction_is_absent_without_replay_slots() {
        let mut publisher = StartupPublisher::default();
        assert_eq!(publisher.fraction(None), None);
    }

    #[test]
    fn test_fraction_never_exceeds_one_or_runs_backwards() {
        let mut publisher = StartupPublisher::default();
        assert_eq!(publisher.fraction(Some((1000, 1100))), Some(0.0));
        assert_eq!(publisher.fraction(Some((1200, 1100))), Some(1.0));
        assert_eq!(publisher.fraction(Some((900, 1100))), Some(0.0));
    }

    #[test]
    fn test_target_already_reached_reads_as_complete() {
        let mut publisher = StartupPublisher::default();
        assert_eq!(publisher.fraction(Some((1000, 1000))), Some(1.0));
    }

    #[test]
    fn test_target_behind_the_origin_reports_nothing() {
        let mut publisher = StartupPublisher::default();
        assert_eq!(publisher.fraction(Some((1000, 900))), None);
    }

    #[test]
    fn test_a_phase_is_timed_from_when_it_was_first_seen() {
        let mut publisher = StartupPublisher::default();
        let base = Instant::now();
        assert_eq!(publisher.elapsed("loading_ledger", base), 0);
        assert_eq!(
            publisher.elapsed("loading_ledger", base + Duration::from_millis(4_500)),
            Duration::from_secs(4).as_nanos() as u64,
            "rounded down to whole seconds, or the debounce sends four a second"
        );
    }

    #[test]
    fn test_a_finished_phase_keeps_what_it_took() {
        let mut publisher = StartupPublisher::default();
        let base = Instant::now();
        publisher.elapsed("cleaning_accounts", base);
        publisher.elapsed("loading_ledger", base + Duration::from_secs(30));

        assert_eq!(publisher.taken.len(), 1);
        assert_eq!(publisher.taken[0].phase, "cleaning_accounts");
        assert_eq!(
            publisher.taken[0].elapsed_nanos,
            Duration::from_secs(30).as_nanos() as u64
        );
    }

    #[test]
    fn test_a_phase_entered_twice_adds_to_its_own_total() {
        // `loading_ledger` is entered once for the snapshot and again if the
        // blockstore has slots to process.
        let mut publisher = StartupPublisher::default();
        let base = Instant::now();
        publisher.elapsed("loading_ledger", base);
        publisher.elapsed("processing_ledger", base + Duration::from_secs(10));
        publisher.elapsed("loading_ledger", base + Duration::from_secs(15));
        publisher.elapsed("processing_ledger", base + Duration::from_secs(21));

        let loading = publisher
            .taken
            .iter()
            .filter(|timing| timing.phase == "loading_ledger")
            .count();
        assert_eq!(
            loading, 1,
            "one row per phase, however often it comes round"
        );
        assert_eq!(
            publisher
                .taken
                .iter()
                .find(|timing| timing.phase == "loading_ledger")
                .unwrap()
                .elapsed_nanos,
            Duration::from_secs(16).as_nanos() as u64,
            "ten seconds the first time and six the second"
        );
    }

    #[test]
    fn test_publishing_fills_in_the_fraction() {
        let publisher = Publisher::new();
        let mut startup = StartupPublisher::default();
        startup.publish(&publisher, replaying(100, 200), None);
        startup.publish(&publisher, replaying(150, 200), None);

        let snapshot = publisher.snapshot();
        assert_eq!(
            snapshot.len(),
            1,
            "startup progress is a single retained key"
        );
        assert!(
            snapshot[0].contains(r#""fraction":0.5"#),
            "expected a half-complete fraction, got {}",
            snapshot[0]
        );
    }

    #[test]
    fn test_the_stake_count_rides_only_on_the_wait() {
        let seen = Some(StakeInGossip {
            online: 3,
            total: 10,
        });
        let waiting = ValidatorStartProgress::WaitingForSupermajority {
            slot: 5,
            gossip_stake_percent: 30,
        };
        for (phase, carried) in [
            (waiting, true),
            (ValidatorStartProgress::Running, false),
            (ValidatorStartProgress::StartingServices, false),
        ] {
            let publisher = Publisher::new();
            StartupPublisher::default().publish(&publisher, phase, seen);
            let sent = publisher.snapshot().pop().unwrap();
            assert_eq!(
                sent.contains(r#""stake_in_gossip":{"online":3,"total":10}"#),
                carried,
                "{sent}"
            );
        }
    }

    #[test]
    fn test_every_phase_is_named_and_only_running_runs() {
        let phases = [
            ValidatorStartProgress::Initializing,
            ValidatorStartProgress::SearchingForRpcService,
            ValidatorStartProgress::CleaningBlockStore,
            ValidatorStartProgress::CleaningAccounts,
            ValidatorStartProgress::LoadingLedger,
            replaying(1, 2),
            ValidatorStartProgress::StartingServices,
            ValidatorStartProgress::Halted,
            ValidatorStartProgress::WaitingForSupermajority {
                slot: 5,
                gossip_stake_percent: 50,
            },
            ValidatorStartProgress::Running,
        ];
        for phase in phases {
            let publisher = Publisher::new();
            StartupPublisher::default().publish(&publisher, phase, None);
            let sent = publisher.snapshot().pop().unwrap();
            assert!(!sent.contains(r#""phase":"""#), "{sent}");
            assert_eq!(
                sent.contains(r#""running":true"#),
                phase == ValidatorStartProgress::Running,
                "{sent}"
            );
        }
    }
}
