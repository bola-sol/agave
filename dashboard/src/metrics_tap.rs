//! Counters lifted from the measurements the validator submits about itself.
//!
//! Some of what an operator most wants to see is held in counters that are
//! private to the module keeping them and swapped to zero as they are reported.
//! Reading them where they live would mean both reaching into another crate and
//! racing the reporter for values only one reader can have. They are already
//! leaving the process as metrics points, so this takes a copy on the way past.
//!
//! The observer runs on whichever validator thread submitted the point, so what
//! happens here is a string comparison against a handful of names and, for the
//! few that match, a scan of their fields into atomics. Nothing allocates,
//! nothing locks, and a point this module does not want costs one comparison.
//!
//! Totals only ever climb. The points themselves carry deltas — each one is what
//! happened since the last was sent — so accumulating them gives a figure that
//! can be differenced between readings, which is what every other rate on the
//! dashboard is built from.

use {
    serde::Serialize,
    solana_metrics::datapoint::DataPoint,
    std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// The point carrying the accounts read cache figures, submitted once a second
/// by the accounts database with the counters reset as it reads them.
const ACCOUNTS_DB_TIMINGS: &str = "accounts_db_store_timings";

/// The two shred receivers, one per socket. Turbine delivers to the first; the
/// second carries only what this validator had to ask another node for, which
/// is what the repair socket is.
const SHREDS_TURBINE: &str = "shred_fetch_receiver";
const SHREDS_REPAIR: &str = "shred_fetch_repair_receiver";

/// The receivers on the other two UDP ports the socket panel lists.
///
/// There is no third. The TPU and TPU forwards ports speak QUIC, where the
/// in-process counters count transactions pulled out of streams rather than
/// datagrams off the wire, and a share worked out from those against a datagram
/// drop count would be a ratio between two different things. The serve repair
/// port does keep a receiver of this kind, but nothing ever reports it: the
/// stats are built, counted into on every packet, and dropped when the service
/// ends. Reaching them would take a change to `core`, which this does not make.
const GOSSIP_RECEIVER: &str = "gossip_receiver";
const TPU_VOTE_RECEIVER: &str = "tpu_vote_receiver";

/// Packets seen, which for the shred receivers is shreds.
const PACKETS_COUNT: &str = "packets_count";

/// The banking stage scheduler's own account of what it did with everything
/// handed to it, reported once a second with its counters reset as it reports.
///
/// This is the whole of the transaction waterfall. The scheduler already counts
/// every transaction that reached it and, for the ones that got no further, the
/// reason — twenty-one figures that between them say where the traffic went.
///
/// Reported only when there is something to report, so an idle validator sends
/// nothing at all rather than a second of zeroes. That is the difference
/// between a scheduler doing nothing and one not being watched, and the panel
/// keeps it: no traffic in the window publishes nothing.
///
/// Worth knowing that this one point does not go through `datapoint_info!`. It
/// calls `solana_metrics::submit` itself, so unlike everything else read here
/// it is not behind the info-logging check and arrives whatever the operator
/// has set their log level to.
const SCHEDULER_COUNTS: &str = "banking_stage_scheduler_counts";

/// Running totals of the counters worth watching.
#[derive(Debug, Default)]
pub struct MetricsTap {
    /// Reads of an account that were already cached, and those that were not.
    pub accounts_cache_hits: AtomicU64,
    pub accounts_cache_misses: AtomicU64,
    /// Accounts dropped from the cache, which is the usual reason a hit rate
    /// falls.
    pub accounts_cache_evicts: AtomicU64,

    /// Shreds that arrived on their own, and shreds this validator had to ask
    /// for. A node the cluster is not reaching gets the second where it should
    /// have had the first.
    ///
    /// The first doubles as the turbine port's received count. That is the same
    /// figure read for a second purpose rather than a second reading of it:
    /// everything arriving on the TVU port is a shred, so what that receiver
    /// counted is what the port delivered.
    pub shreds_turbine: AtomicU64,
    pub shreds_repair: AtomicU64,

    /// Packets delivered on the gossip and TPU vote ports.
    ///
    /// Wanted for the denominator the kernel will not give. `/proc/net/udp`
    /// counts the datagrams a socket discarded but not the ones it handed over,
    /// so a drop count on its own cannot be turned into a share of the traffic —
    /// and a number of drops that cannot be judged against anything is the
    /// weakest thing the socket panel shows. These are the other half of that
    /// sum, and they count datagrams too, one per packet, which is what makes
    /// them addable to a drop count in the first place.
    pub packets_gossip: AtomicU64,
    pub packets_tpu_vote: AtomicU64,

    /// Where the transactions handed to the banking stage ended up.
    pub scheduler: SchedulerCounters,
}

/// The banking stage scheduler's counters, in the order a transaction meets
/// them.
///
/// Three stages with losses between them. Everything sigverify passes on is
/// `received`; what survives the checks at the door is `buffered`; what the
/// scheduler then hands a worker is `scheduled`; what comes back done is
/// `finished`. The rest of these are the reasons the count falls between one
/// stage and the next.
///
/// The first stretch is an identity, and one the validator's own tests assert:
/// received equals buffered plus every drop from `not_held` down to
/// `nonce_conflict`, plus `check_queue_full`. The later stretches are not, and
/// cannot be — the container holds a standing population, so a transaction
/// buffered in one second is scheduled in another, and over any window the
/// three stages are three different populations that merely resemble each
/// other. Reading it as a strict funnel would be wrong.
#[derive(Debug, Default)]
pub struct SchedulerCounters {
    /// Everything sigverify handed the scheduler.
    pub received: AtomicU64,

    // Lost at the door, before ever being buffered.
    /// Not held, because the validator was forwarding rather than buffering.
    /// The ordinary state of a validator that is not near its leader slot, and
    /// on most nodes most of the time this is nearly all of the traffic.
    pub not_held: AtomicU64,
    /// The queue feeding the checks was full.
    pub check_queue_full: AtomicU64,
    /// Would not parse, or would not sanitize.
    pub unparsable: AtomicU64,
    /// Asked for locks it could not have.
    pub bad_locks: AtomicU64,
    /// Its compute budget instructions did not add up.
    pub compute_budget: AtomicU64,
    /// Its blockhash was too old, or its nonce did not hold.
    pub too_old: AtomicU64,
    /// Already in the ledger.
    pub already_processed: AtomicU64,
    /// The fee payer could not pay.
    pub fee_payer: AtomicU64,
    /// Excluded by the account key filter.
    pub filtered: AtomicU64,
    /// A nonce transaction already queued at the same or higher priority.
    pub nonce_conflict: AtomicU64,

    /// Made it into the container.
    pub buffered: AtomicU64,

    // Lost from the container, after being buffered.
    /// Pushed out by something of higher priority when the queue was full.
    pub queue_full: AtomicU64,
    /// Evicted by a validated nonce transaction that outranked it.
    pub nonce_evicted: AtomicU64,
    /// Thrown away when the container was cleared.
    pub cleared: AtomicU64,
    /// Thrown away as stale when the container was cleaned.
    pub cleaned: AtomicU64,

    /// Handed to a worker.
    pub scheduled: AtomicU64,
    /// Held back this pass because it wanted accounts already being written,
    /// or because every worker was busy. Not losses — pressure. These say the
    /// scheduler had work it could not place.
    pub blocked_conflicts: AtomicU64,
    pub blocked_threads: AtomicU64,

    /// Came back from a worker done, and came back to be tried again.
    pub finished: AtomicU64,
    pub retried: AtomicU64,
}

/// A snapshot of [`SchedulerCounters`], for differencing between readings.
///
/// Sent to the browser as it stands, once a window of these has been summed.
/// Deliberately not copied into a separate wire type on the way: the field
/// names here are already the panel's own vocabulary rather than the
/// scheduler's, and twenty-one lines of assigning one to the other would be
/// twenty-one chances to cross a pair over silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct SchedulerTotals {
    pub received: u64,
    pub not_held: u64,
    pub check_queue_full: u64,
    pub unparsable: u64,
    pub bad_locks: u64,
    pub compute_budget: u64,
    pub too_old: u64,
    pub already_processed: u64,
    pub fee_payer: u64,
    pub filtered: u64,
    pub nonce_conflict: u64,
    pub buffered: u64,
    pub queue_full: u64,
    pub nonce_evicted: u64,
    pub cleared: u64,
    pub cleaned: u64,
    pub scheduled: u64,
    pub blocked_conflicts: u64,
    pub blocked_threads: u64,
    pub finished: u64,
    pub retried: u64,
}

/// A snapshot of the totals, for differencing between readings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TapCounters {
    pub accounts_cache_hits: u64,
    pub accounts_cache_misses: u64,
    pub accounts_cache_evicts: u64,
    pub shreds_turbine: u64,
    pub shreds_repair: u64,
    pub packets_gossip: u64,
    pub packets_tpu_vote: u64,
    pub scheduler: SchedulerTotals,
}

impl MetricsTap {
    /// Starts watching, if nothing else already is.
    ///
    /// There is one observer for the process and the first to ask keeps it, so
    /// a second dashboard in the same binary — which does not happen, but the
    /// interface allows it — gets a tap that stays at zero rather than one that
    /// quietly steals the first's.
    pub fn install() -> Arc<Self> {
        let tap = Arc::new(Self::default());
        let observer = tap.clone();
        if !solana_metrics::set_datapoint_observer(Box::new(move |point| {
            observer.observe(point);
        })) {
            log::warn!("dashboard: something else is already watching metrics points");
        }
        tap
    }

    /// Adds what one point carries, if it is one of the few worth reading.
    ///
    /// A point this module does not want leaves after the match below, which is
    /// a comparison against six names. Everything the validator measures about
    /// itself arrives here, so that is the cost paid on every one of them.
    fn observe(&self, point: &DataPoint) {
        match point.name {
            ACCOUNTS_DB_TIMINGS => {
                for (name, value) in &point.fields {
                    let counter = match *name {
                        "read_only_accounts_cache_hits" => &self.accounts_cache_hits,
                        "read_only_accounts_cache_misses" => &self.accounts_cache_misses,
                        "read_only_accounts_cache_evicts" => &self.accounts_cache_evicts,
                        _ => continue,
                    };
                    add_field(counter, value);
                }
            }
            SHREDS_TURBINE => self.add_packets(&self.shreds_turbine, point),
            SHREDS_REPAIR => self.add_packets(&self.shreds_repair, point),
            GOSSIP_RECEIVER => self.add_packets(&self.packets_gossip, point),
            TPU_VOTE_RECEIVER => self.add_packets(&self.packets_tpu_vote, point),
            SCHEDULER_COUNTS => {
                let counters = &self.scheduler;
                for (name, value) in &point.fields {
                    // Named on the left as the scheduler names them and on the
                    // right as the panel reads them. The two vocabularies are
                    // kept apart deliberately: this is the only place that has
                    // to change if a counter is renamed upstream, and every
                    // rename upstream is silent — a field that stops matching
                    // simply stops being counted.
                    let counter = match *name {
                        "num_received" => &counters.received,
                        "num_dropped_on_receive" => &counters.not_held,
                        "num_dropped_on_check_work_queue_full" => &counters.check_queue_full,
                        "num_dropped_on_parsing_and_sanitization" => &counters.unparsable,
                        "num_dropped_on_validate_locks" => &counters.bad_locks,
                        "num_dropped_on_receive_compute_budget" => &counters.compute_budget,
                        "num_dropped_on_receive_age" => &counters.too_old,
                        "num_dropped_on_receive_already_processed" => &counters.already_processed,
                        "num_dropped_on_receive_fee_payer" => &counters.fee_payer,
                        "num_dropped_on_filter_key" => &counters.filtered,
                        "num_dropped_on_nonce_dedup" => &counters.nonce_conflict,
                        "num_buffered" => &counters.buffered,
                        "num_dropped_on_capacity" => &counters.queue_full,
                        "num_evicted_on_nonce_dedup" => &counters.nonce_evicted,
                        "num_dropped_on_clear" => &counters.cleared,
                        "num_dropped_on_clean" => &counters.cleaned,
                        "num_scheduled" => &counters.scheduled,
                        "num_unschedulable_conflicts" => &counters.blocked_conflicts,
                        "num_unschedulable_threads" => &counters.blocked_threads,
                        "num_finished" => &counters.finished,
                        "num_retryable" => &counters.retried,
                        // `min_priority` and `max_priority` are gauges rather
                        // than counts, and accumulating them would be
                        // meaningless.
                        _ => continue,
                    };
                    add_field(counter, value);
                }
            }
            _ => (),
        }
    }

    /// Adds a socket receiver's packet count, the whole of what is wanted from
    /// any of those four points.
    fn add_packets(&self, counter: &AtomicU64, point: &DataPoint) {
        for (name, value) in &point.fields {
            if *name == PACKETS_COUNT {
                add_field(counter, value);
                return;
            }
        }
    }

    /// The totals as they stand, read together so a reading is coherent enough
    /// to difference.
    pub fn counters(&self) -> TapCounters {
        TapCounters {
            accounts_cache_hits: self.accounts_cache_hits.load(Ordering::Relaxed),
            accounts_cache_misses: self.accounts_cache_misses.load(Ordering::Relaxed),
            accounts_cache_evicts: self.accounts_cache_evicts.load(Ordering::Relaxed),
            shreds_turbine: self.shreds_turbine.load(Ordering::Relaxed),
            shreds_repair: self.shreds_repair.load(Ordering::Relaxed),
            packets_gossip: self.packets_gossip.load(Ordering::Relaxed),
            packets_tpu_vote: self.packets_tpu_vote.load(Ordering::Relaxed),
            scheduler: self.scheduler.totals(),
        }
    }
}

impl SchedulerCounters {
    /// The counters as they stand.
    fn totals(&self) -> SchedulerTotals {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        SchedulerTotals {
            received: read(&self.received),
            not_held: read(&self.not_held),
            check_queue_full: read(&self.check_queue_full),
            unparsable: read(&self.unparsable),
            bad_locks: read(&self.bad_locks),
            compute_budget: read(&self.compute_budget),
            too_old: read(&self.too_old),
            already_processed: read(&self.already_processed),
            fee_payer: read(&self.fee_payer),
            filtered: read(&self.filtered),
            nonce_conflict: read(&self.nonce_conflict),
            buffered: read(&self.buffered),
            queue_full: read(&self.queue_full),
            nonce_evicted: read(&self.nonce_evicted),
            cleared: read(&self.cleared),
            cleaned: read(&self.cleaned),
            scheduled: read(&self.scheduled),
            blocked_conflicts: read(&self.blocked_conflicts),
            blocked_threads: read(&self.blocked_threads),
            finished: read(&self.finished),
            retried: read(&self.retried),
        }
    }
}

impl SchedulerTotals {
    /// This reading less the one before it, which is the work of one interval.
    ///
    /// Saturating throughout. The totals only climb, so a lower reading than
    /// the last means the tap was installed mid-flight or a counter was reset
    /// under it, and nought is the right answer to that rather than a number
    /// near `u64::MAX`.
    pub fn since(&self, previous: &Self) -> Self {
        let step = |current: u64, before: u64| current.saturating_sub(before);
        Self {
            received: step(self.received, previous.received),
            not_held: step(self.not_held, previous.not_held),
            check_queue_full: step(self.check_queue_full, previous.check_queue_full),
            unparsable: step(self.unparsable, previous.unparsable),
            bad_locks: step(self.bad_locks, previous.bad_locks),
            compute_budget: step(self.compute_budget, previous.compute_budget),
            too_old: step(self.too_old, previous.too_old),
            already_processed: step(self.already_processed, previous.already_processed),
            fee_payer: step(self.fee_payer, previous.fee_payer),
            filtered: step(self.filtered, previous.filtered),
            nonce_conflict: step(self.nonce_conflict, previous.nonce_conflict),
            buffered: step(self.buffered, previous.buffered),
            queue_full: step(self.queue_full, previous.queue_full),
            nonce_evicted: step(self.nonce_evicted, previous.nonce_evicted),
            cleared: step(self.cleared, previous.cleared),
            cleaned: step(self.cleaned, previous.cleaned),
            scheduled: step(self.scheduled, previous.scheduled),
            blocked_conflicts: step(self.blocked_conflicts, previous.blocked_conflicts),
            blocked_threads: step(self.blocked_threads, previous.blocked_threads),
            finished: step(self.finished, previous.finished),
            retried: step(self.retried, previous.retried),
        }
    }

    /// This reading added to another, for summing a window of them.
    pub fn plus(&self, other: &Self) -> Self {
        let sum = |a: u64, b: u64| a.saturating_add(b);
        Self {
            received: sum(self.received, other.received),
            not_held: sum(self.not_held, other.not_held),
            check_queue_full: sum(self.check_queue_full, other.check_queue_full),
            unparsable: sum(self.unparsable, other.unparsable),
            bad_locks: sum(self.bad_locks, other.bad_locks),
            compute_budget: sum(self.compute_budget, other.compute_budget),
            too_old: sum(self.too_old, other.too_old),
            already_processed: sum(self.already_processed, other.already_processed),
            fee_payer: sum(self.fee_payer, other.fee_payer),
            filtered: sum(self.filtered, other.filtered),
            nonce_conflict: sum(self.nonce_conflict, other.nonce_conflict),
            buffered: sum(self.buffered, other.buffered),
            queue_full: sum(self.queue_full, other.queue_full),
            nonce_evicted: sum(self.nonce_evicted, other.nonce_evicted),
            cleared: sum(self.cleared, other.cleared),
            cleaned: sum(self.cleaned, other.cleaned),
            scheduled: sum(self.scheduled, other.scheduled),
            blocked_conflicts: sum(self.blocked_conflicts, other.blocked_conflicts),
            blocked_threads: sum(self.blocked_threads, other.blocked_threads),
            finished: sum(self.finished, other.finished),
            retried: sum(self.retried, other.retried),
        }
    }
}

/// Adds a field's value to a counter, if it reads as an integer.
fn add_field(counter: &AtomicU64, value: &str) {
    if let Some(delta) = field_u64(value) {
        counter.fetch_add(delta, Ordering::Relaxed);
    }
}

/// Reads a field value that was written as an integer.
///
/// Values arrive formatted for the line protocol the metrics writer speaks
/// rather than as numbers: an integer field carries a trailing `i`, which is
/// InfluxDB's way of saying it is not a float. A value without one is a float,
/// a boolean or a quoted string, and none of those are counters.
fn field_u64(value: &str) -> Option<u64> {
    value.strip_suffix('i')?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(name: &'static str, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = DataPoint::new(name);
        for (field, value) in fields {
            point.fields.push((field, (*value).to_string()));
        }
        point
    }

    fn point(fields: &[(&'static str, &str)]) -> DataPoint {
        named(ACCOUNTS_DB_TIMINGS, fields)
    }

    #[test]
    fn test_integer_fields_carry_the_line_protocol_suffix() {
        // What `add_field_i64` writes, which is not what a reader expects.
        assert_eq!(field_u64("42i"), Some(42));
        assert_eq!(field_u64("0i"), Some(0));
    }

    #[test]
    fn test_anything_that_is_not_an_integer_field_is_left_alone() {
        // A float, a boolean and a quoted string all appear in the same points.
        assert_eq!(field_u64("42"), None);
        assert_eq!(field_u64("1.5"), None);
        assert_eq!(field_u64("true"), None);
        assert_eq!(field_u64("\"words\"i"), None);
    }

    #[test]
    fn test_the_totals_accumulate_across_points() {
        // Each point carries what happened since the last, the counters behind
        // them being reset as they are read, so the totals are the sum.
        let tap = MetricsTap::default();
        tap.observe(&point(&[
            ("read_only_accounts_cache_hits", "10i"),
            ("read_only_accounts_cache_misses", "2i"),
            ("read_only_accounts_cache_evicts", "1i"),
        ]));
        tap.observe(&point(&[
            ("read_only_accounts_cache_hits", "5i"),
            ("read_only_accounts_cache_misses", "1i"),
        ]));

        assert_eq!(
            tap.counters(),
            TapCounters {
                accounts_cache_hits: 15,
                accounts_cache_misses: 3,
                accounts_cache_evicts: 1,
                // Spelled out rather than defaulted: that a point from one
                // source leaves another source's counters alone is part of what
                // this is checking.
                shreds_turbine: 0,
                shreds_repair: 0,
                packets_gossip: 0,
                packets_tpu_vote: 0,
                scheduler: SchedulerTotals::default(),
            }
        );
    }

    #[test]
    fn test_other_points_are_ignored() {
        // The observer sees everything the process submits, most of which is
        // nothing to do with this.
        let tap = MetricsTap::default();
        let mut other = DataPoint::new("banking_stage-loop-stats");
        other
            .fields
            .push(("read_only_accounts_cache_hits", "99i".to_string()));
        tap.observe(&other);
        assert_eq!(tap.counters(), TapCounters::default());
    }

    #[test]
    fn test_shreds_are_counted_by_the_socket_they_arrived_on() {
        // The whole of the repair signal: one receiver per socket, and the
        // repair one carries only what this validator had to ask for.
        let tap = MetricsTap::default();
        tap.observe(&named(SHREDS_TURBINE, &[("packets_count", "900i")]));
        tap.observe(&named(SHREDS_REPAIR, &[("packets_count", "12i")]));
        tap.observe(&named(SHREDS_TURBINE, &[("packets_count", "100i")]));

        let counters = tap.counters();
        assert_eq!(counters.shreds_turbine, 1_000);
        assert_eq!(counters.shreds_repair, 12);
    }

    #[test]
    fn test_each_socket_receiver_counts_into_its_own_port() {
        // The socket panel's denominator. Four receivers report packets under
        // the same field name, and a row's share of traffic lost is only right
        // if each one lands against the port it was read from.
        let tap = MetricsTap::default();
        tap.observe(&named(SHREDS_TURBINE, &[("packets_count", "900i")]));
        tap.observe(&named(GOSSIP_RECEIVER, &[("packets_count", "40i")]));
        tap.observe(&named(TPU_VOTE_RECEIVER, &[("packets_count", "70i")]));
        tap.observe(&named(GOSSIP_RECEIVER, &[("packets_count", "2i")]));

        let counters = tap.counters();
        assert_eq!(counters.shreds_turbine, 900);
        assert_eq!(counters.packets_gossip, 42);
        assert_eq!(counters.packets_tpu_vote, 70);
    }

    /// The waterfall point as the scheduler sends it, one field per counter.
    fn scheduler(fields: &[(&'static str, &str)]) -> DataPoint {
        named(SCHEDULER_COUNTS, fields)
    }

    #[test]
    fn test_the_waterfall_counters_land_where_they_belong() {
        // Twenty-one fields under names that do not resemble the ones the panel
        // uses, several of which differ from each other only in their tail. A
        // pair transposed here would put the fee payer failures under "too old"
        // and nothing downstream could tell.
        let tap = MetricsTap::default();
        tap.observe(&scheduler(&[
            ("num_received", "1000i"),
            ("num_dropped_on_receive", "900i"),
            ("num_dropped_on_check_work_queue_full", "1i"),
            ("num_dropped_on_parsing_and_sanitization", "2i"),
            ("num_dropped_on_validate_locks", "3i"),
            ("num_dropped_on_receive_compute_budget", "4i"),
            ("num_dropped_on_receive_age", "5i"),
            ("num_dropped_on_receive_already_processed", "6i"),
            ("num_dropped_on_receive_fee_payer", "7i"),
            ("num_dropped_on_filter_key", "8i"),
            ("num_dropped_on_nonce_dedup", "9i"),
            ("num_buffered", "55i"),
            ("num_dropped_on_capacity", "10i"),
            ("num_evicted_on_nonce_dedup", "11i"),
            ("num_dropped_on_clear", "12i"),
            ("num_dropped_on_clean", "13i"),
            ("num_scheduled", "40i"),
            ("num_unschedulable_conflicts", "14i"),
            ("num_unschedulable_threads", "15i"),
            ("num_finished", "38i"),
            ("num_retryable", "16i"),
        ]));

        let counters = tap.counters().scheduler;
        assert_eq!(counters.received, 1_000);
        assert_eq!(counters.not_held, 900);
        assert_eq!(counters.check_queue_full, 1);
        assert_eq!(counters.unparsable, 2);
        assert_eq!(counters.bad_locks, 3);
        assert_eq!(counters.compute_budget, 4);
        assert_eq!(counters.too_old, 5);
        assert_eq!(counters.already_processed, 6);
        assert_eq!(counters.fee_payer, 7);
        assert_eq!(counters.filtered, 8);
        assert_eq!(counters.nonce_conflict, 9);
        assert_eq!(counters.buffered, 55);
        assert_eq!(counters.queue_full, 10);
        assert_eq!(counters.nonce_evicted, 11);
        assert_eq!(counters.cleared, 12);
        assert_eq!(counters.cleaned, 13);
        assert_eq!(counters.scheduled, 40);
        assert_eq!(counters.blocked_conflicts, 14);
        assert_eq!(counters.blocked_threads, 15);
        assert_eq!(counters.finished, 38);
        assert_eq!(counters.retried, 16);
    }

    #[test]
    fn test_the_receive_stretch_of_that_point_balances() {
        // The identity the validator's own tests assert, restated against the
        // names this module gives them: everything received either got in or
        // has a reason it did not. Checking it here is what makes the panel's
        // first section a genuine account rather than a list that happens to
        // sit under a heading.
        let counters = MetricsTap::default();
        counters.observe(&scheduler(&[
            ("num_received", "1000i"),
            ("num_dropped_on_receive", "900i"),
            ("num_dropped_on_check_work_queue_full", "1i"),
            ("num_dropped_on_parsing_and_sanitization", "2i"),
            ("num_dropped_on_validate_locks", "3i"),
            ("num_dropped_on_receive_compute_budget", "4i"),
            ("num_dropped_on_receive_age", "5i"),
            ("num_dropped_on_receive_already_processed", "6i"),
            ("num_dropped_on_receive_fee_payer", "7i"),
            ("num_dropped_on_filter_key", "8i"),
            ("num_dropped_on_nonce_dedup", "9i"),
            ("num_buffered", "55i"),
        ]));

        let totals = counters.counters().scheduler;
        let accounted = [
            totals.not_held,
            totals.check_queue_full,
            totals.unparsable,
            totals.bad_locks,
            totals.compute_budget,
            totals.too_old,
            totals.already_processed,
            totals.fee_payer,
            totals.filtered,
            totals.nonce_conflict,
            totals.buffered,
        ]
        .into_iter()
        .fold(0u64, u64::saturating_add);
        assert_eq!(accounted, totals.received);
    }

    #[test]
    fn test_a_window_of_readings_differences_and_sums() {
        // How the panel is built: totals that only climb, differenced against
        // the last reading to give one interval's work, then added across the
        // window. Both halves saturate, so a counter that went backwards under
        // the tap reads as no work rather than as eighteen quintillion.
        let first = SchedulerTotals {
            received: 100,
            buffered: 10,
            ..SchedulerTotals::default()
        };
        let second = SchedulerTotals {
            received: 250,
            buffered: 25,
            ..SchedulerTotals::default()
        };

        let step = second.since(&first);
        assert_eq!(step.received, 150);
        assert_eq!(step.buffered, 15);

        assert_eq!(step.plus(&step).received, 300);
        // Backwards, which only happens if a counter was reset under us.
        assert_eq!(first.since(&second).received, 0);
    }

    #[test]
    fn test_the_priority_gauges_are_left_out_of_the_waterfall() {
        // They ride the same point and are the only two fields on it that are
        // not counts. Summing a window of "the highest fee in the queue right
        // now" would produce a number with no meaning at all.
        let tap = MetricsTap::default();
        tap.observe(&scheduler(&[
            ("min_priority", "5i"),
            ("max_priority", "900000i"),
            ("num_received", "7i"),
        ]));
        assert_eq!(tap.counters().scheduler.received, 7);
    }

    #[test]
    fn test_only_the_packet_count_is_taken_from_a_receiver() {
        // Those points carry timings and channel depths as well, and adding
        // those to a shred count would be nonsense rather than merely wrong.
        let tap = MetricsTap::default();
        tap.observe(&named(
            SHREDS_TURBINE,
            &[
                ("packet_batches_count", "7i"),
                ("packets_count", "900i"),
                ("channel_len", "3i"),
            ],
        ));
        assert_eq!(tap.counters().shreds_turbine, 900);
    }
}
