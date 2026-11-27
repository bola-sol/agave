//! Our blocks, turns, per-slot reports, every slot's row and host readings read back from the rewind
//! log at startup, so a restart keeps the history the pages draw.

use {
    crate::{
        catch_up::IN_STEP_SLOTS,
        history::{PACKED_SLOTS, ReadBack},
        host_history::HostHistory,
        metrics_tap::{SlotCost, SlotWaterfall, keep_newest},
        produced::{BlockFees, MAX_HELD_BLOCKS, ProducedBlock},
        rewind_log::RewindLog,
        slots::SlotEntry,
        turns::LeaderTurn,
    },
    serde::de::DeserializeOwned,
    solana_clock::Slot,
    std::collections::BTreeMap,
};

const PREFIX: &str = r#"{"topic":"summary","key":""#;

const SLOT_PREFIX: &str = r#"{"topic":"slot","key":""#;

const VALUE: &str = r#"","value":"#;

/// What the log held of ours, each slot as last recorded.
#[derive(Default)]
pub struct Refilled {
    pub blocks: BTreeMap<Slot, ProducedBlock>,
    pub turns: BTreeMap<Slot, LeaderTurn>,
    pub waterfalls: BTreeMap<Slot, SlotWaterfall>,
    pub costs: BTreeMap<Slot, SlotCost>,
    pub fees: BTreeMap<Slot, BlockFees>,
    /// The host's readings, binned by when each was recorded; `None` with none read, and once the
    /// service has taken them.
    pub host: Option<HostHistory>,
    /// Every slot's row as last recorded; `None` with none read, and once the service has taken it.
    pub slots: Option<SlotFill>,
    /// Slots read back below rooted, newest first, for the collector to have the ledger judge.
    pub unsettled: Vec<Slot>,
    pub segments: usize,
    /// Lines with one of our keys that did not parse.
    pub unread: usize,
}

/// Reads `segments`, as `RewindLog::range` lists them, oldest first; `stop` is asked between
/// segments.
pub fn read(log: &RewindLog, segments: &[(u64, u64, usize)], stop: impl Fn() -> bool) -> Refilled {
    let mut refilled = Refilled::default();
    for (start, _, blocks) in segments {
        if stop() {
            break;
        }
        if let Some(host) = &mut refilled.host {
            host.begin_segment();
        }
        for block in 0..*blocks {
            // A segment pruned since it was listed reads as nothing.
            let Some(block) = log.read(*start, block) else {
                break;
            };
            for line in block.text.lines() {
                refilled.apply(line, block.keyframe);
            }
        }
        refilled.segments = refilled.segments.saturating_add(1);
    }
    refilled.finish();
    refilled
}

/// A slot line's key and its value's text.
fn split_slot(line: &str) -> Option<(&str, &str)> {
    let (_, json) = line.split_once(' ')?;
    let (key, value) = json.strip_prefix(SLOT_PREFIX)?.split_once(VALUE)?;
    Some((key, value.strip_suffix('}')?))
}

/// The slot rows a recording holds, and which run of the process is being read and whether it was
/// still catching up then. A run begins where the start time changes.
pub struct SlotFill {
    pub read_back: ReadBack,
    run: u16,
    started: Option<u64>,
    catching_up: bool,
    caught: bool,
    /// Slot entries read, and those that did not parse.
    pub entries: usize,
}

impl SlotFill {
    fn new() -> Self {
        Self {
            read_back: ReadBack::new(PACKED_SLOTS),
            run: 0,
            started: None,
            catching_up: false,
            caught: false,
            entries: 0,
        }
    }

    fn note_start(&mut self, nanos: u64) {
        if self.started != Some(nanos) {
            self.started = Some(nanos);
            self.run = self.run.wrapping_add(1);
            self.catching_up = false;
            self.caught = false;
        }
    }

    fn note_behind(&mut self, behind: Option<u64>) {
        if !self.caught && behind.is_some_and(|slots| slots > IN_STEP_SLOTS) {
            self.catching_up = true;
        }
    }

    fn note_caught(&mut self) {
        self.caught = true;
        self.catching_up = false;
    }

    fn apply(&mut self, entry: &SlotEntry) {
        self.read_back.apply(entry, self.run, self.catching_up);
        self.entries = self.entries.saturating_add(1);
    }
}

/// When a summary line was recorded, its key and its value's text, the rest unparsed.
fn split(line: &str) -> Option<(u64, &str, &str)> {
    let (at_ms, json) = line.split_once(' ')?;
    let (key, value) = json.strip_prefix(PREFIX)?.split_once(VALUE)?;
    Some((at_ms.parse().ok()?, key, value.strip_suffix('}')?))
}

fn parse<T: DeserializeOwned>(value: &str) -> Option<T> {
    serde_json::from_str(value).ok()
}

impl Refilled {
    /// `opening` for a line of a segment's own copy of what a joining page is sent.
    fn apply(&mut self, line: &str, opening: bool) {
        let Some((at_ms, key, value)) = split(line) else {
            self.apply_slot(line);
            return;
        };
        let read = match key {
            "startup_time_nanos" => parse(value).map(|nanos: u64| self.fill().note_start(nanos)),
            "behind_cluster" => {
                parse(value).map(|behind: Option<u64>| self.fill().note_behind(behind))
            }
            "caught_up_time_nanos" => parse(value).map(|_: u64| self.fill().note_caught()),
            "host" | "network" | "ingest_paths" => self
                .host
                .get_or_insert_with(HostHistory::new)
                .note_recorded(at_ms, key, value, opening)
                .and_then(|read| read.then_some(())),
            "produced_block" => parse(value).map(|block: ProducedBlock| {
                self.blocks.insert(block.slot, block);
            }),
            "produced_blocks" => parse(value).map(|list| self.whole(list)),
            "produced_turns" => parse(value).map(|turns: Vec<LeaderTurn>| {
                self.turns
                    .extend(turns.into_iter().map(|turn| (turn.first, turn)));
            }),
            "produced_floor" => parse(value).map(|floor| self.hold_from(floor)),
            "produced_dropped" => parse(value).map(|slots: Vec<Slot>| {
                for slot in slots {
                    self.blocks.remove(&slot);
                }
            }),
            "slot_waterfall" => parse(value).map(|waterfall: SlotWaterfall| {
                self.waterfalls.insert(waterfall.slot, waterfall);
            }),
            "slot_waterfalls" => parse(value).map(|list: Vec<SlotWaterfall>| {
                self.waterfalls.extend(
                    list.into_iter()
                        .map(|waterfall| (waterfall.slot, waterfall)),
                );
            }),
            "slot_cost" => parse(value).map(|cost: SlotCost| {
                self.costs.insert(cost.slot, cost);
            }),
            "slot_costs" => parse(value).map(|list: Vec<SlotCost>| {
                self.costs
                    .extend(list.into_iter().map(|cost| (cost.slot, cost)));
            }),
            "block_fees" => parse(value).map(|fees: BlockFees| {
                self.fees.insert(fees.slot, fees);
            }),
            _ => return,
        };
        if read.is_none() {
            self.unread = self.unread.saturating_add(1);
        }
    }

    fn fill(&mut self) -> &mut SlotFill {
        self.slots.get_or_insert_with(SlotFill::new)
    }

    /// A slot's entry, or the overview of the newest, each as the collector last sent it.
    fn apply_slot(&mut self, line: &str) {
        let Some((key, value)) = split_slot(line) else {
            return;
        };
        let read = match key {
            "update" => parse(value).map(|entry: SlotEntry| self.fill().apply(&entry)),
            "overview" => parse(value).map(|entries: Vec<SlotEntry>| {
                let fill = self.fill();
                for entry in &entries {
                    fill.apply(entry);
                }
            }),
            _ => return,
        };
        if read.is_none() {
            self.unread = self.unread.saturating_add(1);
        }
    }

    /// The newest blocks as the store held them, so one missing from their span had been dropped.
    fn whole(&mut self, list: Vec<ProducedBlock>) {
        let (Some(first), Some(last)) = (list.first(), list.last()) else {
            return;
        };
        let mut span = self.blocks.split_off(&first.slot);
        let mut above = span.split_off(&last.slot.saturating_add(1));
        self.blocks.append(&mut above);
        self.blocks
            .extend(list.into_iter().map(|block| (block.slot, block)));
    }

    fn hold_from(&mut self, floor: Slot) {
        self.blocks = self.blocks.split_off(&floor);
        self.turns = self.turns.split_off(&floor);
        self.waterfalls = self.waterfalls.split_off(&floor);
        self.costs = self.costs.split_off(&floor);
        self.fees = self.fees.split_off(&floor);
    }

    fn finish(&mut self) {
        keep_newest(&mut self.blocks, MAX_HELD_BLOCKS);
        keep_newest(&mut self.waterfalls, MAX_HELD_BLOCKS);
        keep_newest(&mut self.costs, MAX_HELD_BLOCKS);
        let blocks = &self.blocks;
        self.fees.retain(|slot, _| blocks.contains_key(slot));
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            metrics_tap::{SchedulerSource, SchedulerTotals},
            produced::{sample_block as block, tests::turn},
            proto::{Publisher, TOPIC_SLOT, TOPIC_SUMMARY},
            rewind_log::{Recorder, Settings},
        },
        std::{
            fs::OpenOptions,
            io::Write,
            path::Path,
            sync::{
                Arc,
                atomic::{AtomicU64, Ordering},
            },
        },
    };

    fn settings() -> Settings {
        Settings {
            segment_ms: 60_000,
            block_raw: 4096,
            block_ms: 5_000,
            retain_ms: 600_000,
            cap_bytes: u64::MAX,
        }
    }

    /// Records what `publish` sends, a step at a time, then reads it all back.
    fn record(dir: &Path, publish: impl FnOnce(&Publisher, &mut dyn FnMut())) -> Refilled {
        record_from(dir, 1_000_000, publish)
    }

    /// One run of the process recording into `dir` from `start`, then everything `dir` holds.
    fn record_from(
        dir: &Path,
        start: u64,
        publish: impl FnOnce(&Publisher, &mut dyn FnMut()),
    ) -> Refilled {
        let log = Arc::new(RewindLog::open(dir).unwrap());
        let publisher = Arc::new(Publisher::new());
        let now = Arc::new(AtomicU64::new(start));
        let clock = now.clone();
        let mut recorder = Recorder::new(
            log,
            publisher.clone(),
            settings(),
            Box::new(move || clock.load(Ordering::Relaxed)),
        );
        let mut step = || {
            recorder.step();
            now.fetch_add(1_000, Ordering::Relaxed);
        };
        step();
        publish(&publisher, &mut step);
        step();
        recorder.close();
        let log = RewindLog::open(dir).unwrap();
        read(&log, &log.range(), || false)
    }

    fn slots<T>(held: &BTreeMap<Slot, T>) -> Vec<Slot> {
        held.keys().copied().collect()
    }

    /// Encoded as the feed sends it, then read back as a refill reads it.
    fn round_trip<T: serde::Serialize + DeserializeOwned>(key: &str, value: &T) -> T {
        let message = crate::proto::encode(TOPIC_SUMMARY, key, value);
        let line = format!("1 {}", message.text());
        let (at_ms, read_key, text) = split(&line).unwrap();
        assert_eq!((at_ms, read_key), (1, key));
        parse(text).unwrap()
    }

    #[test]
    fn test_every_recorded_type_reads_back_as_sent() {
        use crate::{
            metrics_tap::{LeaderTiming, StageTimes},
            produced::{
                BlockCertificate, Bundles, CertificateValidator, Execution, TopFeePayer,
                TopFeeTransaction,
            },
            versions::TxVersions,
        };
        let ip: std::net::IpAddr = "1.1.1.1".parse().unwrap();
        let mut full = block(10);
        full.slot_time_millis = Some(4_000);
        full.duration_nanos = Some(400_000_000);
        full.tips = Some(7);
        full.bundles = Some(Bundles {
            sanitized: 3,
            executed: 2,
        });
        full.versions = Some(TxVersions {
            legacy: 1,
            v0: 2,
            v1: 3,
        });
        full.execution = Some(Execution {
            non_vote: StageTimes {
                load_execute: 9,
                ..StageTimes::default()
            },
            workers: 4,
            longest_batch: 5,
            votes: Some(StageTimes::default()),
            window_millis: 402,
        });
        full.certificate = Some(BlockCertificate {
            rewards: 12,
            leader: Some(solana_pubkey::Pubkey::new_unique()),
            leader_name: Some("leader".to_owned()),
            notarized: true,
            paid: 3,
            ranks: 4,
            stake_paid: 0.75,
            notar: 3,
            skip: 1,
            ours_in: true,
            usual: Some(3),
            left_out: vec![CertificateValidator {
                identity: solana_pubkey::Pubkey::new_unique(),
                name: None,
                ip: Some(ip),
                country: crate::geoip::country(ip),
            }],
        });
        full.leading = Some(LeaderTiming {
            open_millis: Some(380),
            fast_handover: true,
            ..LeaderTiming::default()
        });
        assert!(
            full.certificate.as_ref().unwrap().left_out[0]
                .country
                .is_some()
        );
        assert_eq!(round_trip("produced_block", &full), full);

        let turn = turn(8, 11);
        assert_eq!(round_trip("produced_turns", &vec![turn.clone()]), [turn]);
        let waterfall = SlotWaterfall {
            slot: 10,
            source: SchedulerSource::Bam,
            counts: SchedulerTotals {
                received: 5,
                ..Default::default()
            },
        };
        assert_eq!(round_trip("slot_waterfall", &waterfall), waterfall);
        let cost = SlotCost {
            slot: 10,
            costliest_account: "account".to_owned(),
            costliest_cost: 4,
            ..SlotCost::default()
        };
        assert_eq!(round_trip("slot_cost", &cost), cost);
        let payer = solana_pubkey::Pubkey::new_unique();
        let fees = BlockFees {
            slot: 10,
            payer_count: 1,
            payers: vec![TopFeePayer {
                payer,
                transactions: 2,
                failed: 1,
                priority: 3,
                paid: 4,
            }],
            transactions: vec![TopFeeTransaction {
                signature: "sig".to_owned(),
                payer,
                failed: None,
                priority: 3,
                paid: 4,
            }],
        };
        assert_eq!(round_trip("block_fees", &fees), fees);
    }

    #[test]
    fn test_the_last_copy_of_a_block_wins() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &block(10));
            step();
            let mut filled = block(10);
            filled.duration_nanos = Some(400_000_000);
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &filled);
        });
        assert_eq!(slots(&refilled.blocks), [10]);
        assert_eq!(refilled.blocks[&10].duration_nanos, Some(400_000_000));
        assert_eq!(refilled.unread, 0);
    }

    #[test]
    fn test_a_block_missing_from_a_later_whole_list_was_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            for slot in [10, 11, 12, 13] {
                publisher.publish_update(TOPIC_SUMMARY, "produced_block", slot, &block(slot));
            }
            step();
            let list = vec![block(10), block(11), block(13)];
            publisher.publish(TOPIC_SUMMARY, "produced_blocks", &list);
        });
        assert_eq!(slots(&refilled.blocks), [10, 11, 13]);
    }

    #[test]
    fn test_a_short_list_after_a_restart_drops_nothing_older() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            for slot in [10, 11] {
                publisher.publish_update(TOPIC_SUMMARY, "produced_block", slot, &block(slot));
            }
            step();
            publisher.publish(TOPIC_SUMMARY, "produced_blocks", &vec![block(20)]);
            step();
            publisher.publish(
                TOPIC_SUMMARY,
                "produced_blocks",
                &Vec::<ProducedBlock>::new(),
            );
        });
        assert_eq!(slots(&refilled.blocks), [10, 11, 20]);
    }

    #[test]
    fn test_a_block_a_restart_turned_away_stays_away() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            for slot in [10, 11, 12] {
                publisher.publish_update(TOPIC_SUMMARY, "produced_block", slot, &block(slot));
            }
            step();
            publisher.publish_ephemeral(TOPIC_SUMMARY, "produced_dropped", &vec![11u64, 12]);
        });
        assert_eq!(slots(&refilled.blocks), [10]);
    }

    #[test]
    fn test_a_floor_drops_what_is_older() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            for slot in [10, 20] {
                publisher.publish_update(TOPIC_SUMMARY, "produced_block", slot, &block(slot));
                publisher.publish_update(
                    TOPIC_SUMMARY,
                    "slot_cost",
                    slot,
                    &SlotCost {
                        slot,
                        ..SlotCost::default()
                    },
                );
            }
            step();
            publisher.publish(TOPIC_SUMMARY, "produced_floor", &15u64);
        });
        assert_eq!(slots(&refilled.blocks), [20]);
        assert_eq!(slots(&refilled.costs), [20]);
    }

    #[test]
    fn test_turns_waterfalls_and_fees_are_read() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, _| {
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &block(10));
            let waterfall = SlotWaterfall {
                slot: 10,
                source: SchedulerSource::Scheduler,
                counts: SchedulerTotals::default(),
            };
            publisher.publish(TOPIC_SUMMARY, "slot_waterfalls", &vec![waterfall]);
            publisher.publish(TOPIC_SUMMARY, "produced_turns", &vec![turn(8, 11)]);
            let fees = |slot| BlockFees {
                slot,
                payer_count: 2,
                payers: Vec::new(),
                transactions: Vec::new(),
            };
            publisher.publish_update(TOPIC_SUMMARY, "block_fees", 10, &fees(10));
            // A block not held keeps no fees.
            publisher.publish_update(TOPIC_SUMMARY, "block_fees", 11, &fees(11));
        });
        assert_eq!(slots(&refilled.waterfalls), [10]);
        assert_eq!(slots(&refilled.turns), [8]);
        assert_eq!(slots(&refilled.fees), [10]);
    }

    #[test]
    fn test_host_readings_are_binned_by_when_they_were_recorded() {
        use crate::{host_history::SeriesKey, meters::Network};

        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            for received_per_second in [100, 200, 300] {
                let network = Network {
                    received_per_second,
                    sent_per_second: 0,
                };
                publisher.publish(TOPIC_SUMMARY, "network", &network);
                step();
            }
            publisher.publish(TOPIC_SUMMARY, "host", &"not a host");
        });
        // The recorder's clock starts at a thousand seconds and moves a second a step.
        let range = refilled
            .host
            .unwrap()
            .range(&[SeriesKey::NetIn], 1_000_000, 1_010_000, 1);
        assert_eq!(range.series[0].mean, [Some(200.0)]);
        assert_eq!(range.series[0].max, [Some(300.0)]);
        assert_eq!(range.held_from, Some(1_000_000));
        assert_eq!(refilled.unread, 1);
    }

    #[test]
    fn test_drops_recorded_by_two_runs_are_not_counted_from_one_to_the_other() {
        use crate::host_history::SeriesKey;

        let ingest = |total: u64| serde_json::json!({ "window_seconds": 60.0, "paths": [{ "port": 8001, "drops_total": total }] });
        let dir = tempfile::tempdir().unwrap();
        record_from(dir.path(), 1_000_000, |publisher, step| {
            publisher.publish(TOPIC_SUMMARY, "ingest_paths", &ingest(30));
            step();
            publisher.publish(TOPIC_SUMMARY, "ingest_paths", &ingest(40));
            step();
        });
        // The next run opens its segment before its meters have read a socket, and its first
        // totals have no baseline taken off them yet.
        let refilled = record_from(dir.path(), 1_100_000, |publisher, step| {
            publisher.publish(TOPIC_SUMMARY, "ingest_paths", &ingest(5_000_050));
            step();
            publisher.publish(TOPIC_SUMMARY, "ingest_paths", &ingest(5_000_060));
            step();
        });
        let host = refilled.host.unwrap();
        let first = host.range(&[SeriesKey::UdpDrops], 1_000_000, 1_010_000, 1);
        assert_eq!(first.series[0].mean, [Some(1.0)]);
        // Ten dropped in the second run, and nothing of the time between the two.
        let rest = host.range(&[SeriesKey::UdpDrops], 1_010_000, 1_110_000, 10);
        let mut expected = vec![None; 10];
        expected[9] = Some(1.0);
        assert_eq!(rest.series[0].mean, expected);
        assert_eq!(rest.series[0].max, {
            let mut max = vec![None; 10];
            max[9] = Some(10.0);
            max
        });
    }

    fn slot_entry(slot: Slot, level: crate::slots::SlotLevel, fees: Option<u64>) -> SlotEntry {
        SlotEntry {
            slot,
            level,
            mine: false,
            block: fees.map(|total_fees| crate::slots::BlockDetail {
                transactions: 100,
                non_vote_transactions: 40,
                failed_transactions: 1,
                entries: 0,
                block_cost: 1_000,
                block_cost_limit: 60_000_000,
                account_cost_limit: 12_000_000,
                total_fees,
                priority_fees: 0,
                tips: None,
                replay_micros: Some(8_000),
            }),
            duration_nanos: None,
            time_millis: Some(1_000_000),
            shreds: None,
            replayed_millis: None,
            reward: None,
            left_out: None,
        }
    }

    fn row(refilled: &Refilled, slot: Slot) -> crate::history::PackedSlot {
        *refilled
            .slots
            .as_ref()
            .expect("slots read")
            .read_back
            .history()
            .get(slot)
            .expect("slot held")
    }

    #[test]
    fn test_each_slot_reads_back_as_its_last_line_left_it() {
        use crate::{history::HAS_BLOCK, slots::SlotLevel};

        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            publisher.publish(TOPIC_SUMMARY, "startup_time_nanos", &1u64);
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                10,
                &slot_entry(10, SlotLevel::Incomplete, None),
            );
            step();
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                10,
                &slot_entry(10, SlotLevel::Finalized, Some(500)),
            );
            publisher.publish_update(TOPIC_SLOT, "update", 11, &"not an entry");
            step();
        });
        let ten = row(&refilled, 10);
        assert_eq!((ten.level, ten.fees), (SlotLevel::Finalized as u8, 500));
        assert_ne!(ten.flags & HAS_BLOCK, 0);
        assert_eq!(ten.time_millis, 1_000_000);
        assert_eq!(refilled.unread, 1);
    }

    #[test]
    fn test_a_later_run_keeps_a_full_row_and_marks_what_it_replayed_catching_up() {
        use crate::{history::CATCH_UP, slots::SlotLevel};

        let dir = tempfile::tempdir().unwrap();
        record_from(dir.path(), 1_000_000, |publisher, step| {
            publisher.publish(TOPIC_SUMMARY, "startup_time_nanos", &1u64);
            publisher.publish(TOPIC_SUMMARY, "caught_up_time_nanos", &1u64);
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                10,
                &slot_entry(10, SlotLevel::Finalized, Some(500)),
            );
            step();
        });
        let refilled = record_from(dir.path(), 1_100_000, |publisher, step| {
            publisher.publish(TOPIC_SUMMARY, "startup_time_nanos", &2u64);
            publisher.publish(TOPIC_SUMMARY, "behind_cluster", &Some(50u64));
            // The seam: the clock walk's line for a slot the first run held whole.
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                10,
                &slot_entry(10, SlotLevel::Skipped, None),
            );
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                11,
                &slot_entry(11, SlotLevel::Finalized, Some(600)),
            );
            step();
            publisher.publish(TOPIC_SUMMARY, "caught_up_time_nanos", &2u64);
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                12,
                &slot_entry(12, SlotLevel::Finalized, Some(700)),
            );
            step();
        });
        assert_eq!(row(&refilled, 10).fees, 500);
        assert_ne!(row(&refilled, 11).flags & CATCH_UP, 0);
        assert_eq!(row(&refilled, 12).flags & CATCH_UP, 0);
    }

    #[test]
    fn test_a_run_that_starts_in_step_marks_nothing_as_catching_up() {
        use crate::{history::CATCH_UP, slots::SlotLevel};

        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, step| {
            publisher.publish(TOPIC_SUMMARY, "startup_time_nanos", &1u64);
            publisher.publish(TOPIC_SUMMARY, "behind_cluster", &Some(1u64));
            publisher.publish_update(
                TOPIC_SLOT,
                "update",
                10,
                &slot_entry(10, SlotLevel::Finalized, Some(500)),
            );
            step();
        });
        assert_eq!(row(&refilled, 10).flags & CATCH_UP, 0);
    }

    #[test]
    fn test_a_line_that_does_not_parse_is_counted_and_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let refilled = record(dir.path(), |publisher, _| {
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &"not a block");
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 11, &block(11));
            publisher.publish(TOPIC_SUMMARY, "cluster", &"testnet");
        });
        assert_eq!(slots(&refilled.blocks), [11]);
        assert_eq!(refilled.unread, 1);
    }

    #[test]
    fn test_a_tail_cut_off_by_a_crash_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        record(dir.path(), |publisher, _| {
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &block(10));
        });
        let path = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&[0x50, 0x2a, 0x4d, 0x18, 1, 2, 3]).unwrap();
        let log = RewindLog::open(dir.path()).unwrap();
        let refilled = read(&log, &log.range(), || false);
        assert_eq!(slots(&refilled.blocks), [10]);
    }

    #[test]
    fn test_a_segment_gone_since_listed_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        record(dir.path(), |publisher, _| {
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &block(10));
        });
        let log = RewindLog::open(dir.path()).unwrap();
        let mut listed = log.range();
        listed.insert(0, (1, 2, 3));
        let refilled = read(&log, &listed, || false);
        assert_eq!(slots(&refilled.blocks), [10]);
        assert_eq!(refilled.segments, 2);
    }

    #[test]
    fn test_stop_ends_the_read() {
        let dir = tempfile::tempdir().unwrap();
        record(dir.path(), |publisher, _| {
            publisher.publish_update(TOPIC_SUMMARY, "produced_block", 10, &block(10));
        });
        let log = RewindLog::open(dir.path()).unwrap();
        let refilled = read(&log, &log.range(), || true);
        assert!(refilled.blocks.is_empty());
        assert_eq!(refilled.segments, 0);
    }

    #[test]
    fn test_the_newest_are_kept_under_the_cap() {
        let mut refilled = Refilled::default();
        let over = u64::try_from(MAX_HELD_BLOCKS).unwrap().saturating_add(2);
        for slot in 0..over {
            refilled.blocks.insert(slot, block(slot));
        }
        refilled.finish();
        assert_eq!(refilled.blocks.len(), MAX_HELD_BLOCKS);
        assert_eq!(refilled.blocks.keys().next(), Some(&2));
    }
}
