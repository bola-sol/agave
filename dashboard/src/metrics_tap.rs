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

/// Running totals of the counters worth watching.
#[derive(Debug, Default)]
pub struct MetricsTap {
    /// Reads of an account that were already cached, and those that were not.
    pub accounts_cache_hits: AtomicU64,
    pub accounts_cache_misses: AtomicU64,
    /// Accounts dropped from the cache, which is the usual reason a hit rate
    /// falls.
    pub accounts_cache_evicts: AtomicU64,
}

/// A snapshot of the totals, for differencing between readings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TapCounters {
    pub accounts_cache_hits: u64,
    pub accounts_cache_misses: u64,
    pub accounts_cache_evicts: u64,
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
    fn observe(&self, point: &DataPoint) {
        if point.name != ACCOUNTS_DB_TIMINGS {
            return;
        }
        for (name, value) in &point.fields {
            let counter = match *name {
                "read_only_accounts_cache_hits" => &self.accounts_cache_hits,
                "read_only_accounts_cache_misses" => &self.accounts_cache_misses,
                "read_only_accounts_cache_evicts" => &self.accounts_cache_evicts,
                _ => continue,
            };
            if let Some(delta) = field_u64(value) {
                counter.fetch_add(delta, Ordering::Relaxed);
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
        }
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

    fn point(fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = DataPoint::new(ACCOUNTS_DB_TIMINGS);
        for (name, value) in fields {
            point.fields.push((name, (*value).to_string()));
        }
        point
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
}
