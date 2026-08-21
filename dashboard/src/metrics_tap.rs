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
    /// a comparison against five names. Everything the validator measures about
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
