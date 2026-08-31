//! A flat history of what each recent slot contained.
//!
//! The slot ring in [`crate::slots`] holds whole [`SlotEntry`] records, which
//! is the right shape for the few hundred slots a client is sent and the wrong
//! one for a hundred thousand: measured at about two hundred and seventy bytes
//! apiece in a map, most of it the leader's key, name and icon repeated for
//! every slot of a turn.
//!
//! This holds the same span in a flat array of fixed-size rows, carrying only
//! the columns the schedule page draws. Nothing reads it yet. It exists so that
//! the depth is being retained before the query that serves it is built, since
//! a history only starts being useful once it has had time to fill.

use {crate::slots::SlotEntry, solana_clock::Slot};

/// One slot, packed to the columns a schedule row draws.
///
/// Thirty-two bytes, and forty in the ring with the slot it belongs to. The
/// leader is not among them: it comes from the epoch's turn array, where it is
/// stored once per leader rather than once per slot.
///
/// Duration is not among them either, and is not missing. It is the gap to the
/// previous slot that has a clock, so a reader holding a span of these works it
/// out the same way the collector does, and storing it would be storing a
/// subtraction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PackedSlot {
    /// [`crate::slots::SlotLevel`] as its discriminant.
    pub level: u8,
    /// Bit 0: a block was recorded. Bit 1: the slot's clock is known.
    ///
    /// Both are needed because nought is a real reading for every count here.
    /// A block that landed empty and a slot whose block was never seen are
    /// different things, and neither is a slot that was skipped.
    pub flags: u8,
    pub votes: u32,
    pub non_votes: u32,
    /// Compute units the block used. Saturating into `u32`, which is seventy
    /// times the current block limit; a cluster that raises the limit past four
    /// billion gets a clamped figure rather than a wrapped one.
    pub compute: u32,
    pub fees: u64,
    /// Wall clock of the slot's first shred, in milliseconds.
    ///
    /// Absolute rather than an offset from the window. An offset would be four
    /// bytes instead of eight and would pack the row to thirty-two with its
    /// slot, but it would also have to be rebased every time the window moved,
    /// and a validator up for more than forty-nine days would overflow it. The
    /// span a reader is sent can still carry offsets: rebasing is a subtraction
    /// at the point of sending, where the range is known.
    pub time_millis: u64,
}

/// Set where the slot recorded a block, as against one that has not frozen or
/// was skipped.
pub const HAS_BLOCK: u8 = 1;
/// Set where the slot's first shred was timed.
pub const HAS_CLOCK: u8 = 1 << 1;

/// A fixed-size history of packed slots, indexed by the slot itself.
///
/// Direct-mapped: the row for a slot is always at `slot % capacity`, so writing
/// and reading are both a single index with no map to walk or rebalance. The
/// slot is stored beside its row rather than implied by the position, which
/// costs eight bytes and removes the one bug this shape invites. Without it a
/// row from a full lap ago is indistinguishable from a current one, and the
/// alternative, clearing rows as the window advances, is a second thing that
/// has to be right for the first to be trusted.
pub struct SlotHistory {
    rows: Vec<(Slot, PackedSlot)>,
}

impl SlotHistory {
    /// Allocates the whole history up front. It never grows and never shrinks,
    /// so this is the only allocation it makes.
    pub fn new(capacity: usize) -> Self {
        Self {
            rows: vec![(0, PackedSlot::default()); capacity.max(1)],
        }
    }

    /// The row for `slot`, or `None` where the history has never held it or has
    /// since lapped past it.
    pub fn get(&self, slot: Slot) -> Option<&PackedSlot> {
        let (held, row) = self.rows.get(self.index(slot))?;
        (*held == slot && slot != 0).then_some(row)
    }

    /// What one slot contained, from the entry the collector already keeps.
    ///
    /// Called on every change to an entry rather than once when it settles: a
    /// slot's level climbs through several values and its block arrives on
    /// freeze, and there is no single moment at which it is finished.
    pub fn record(&mut self, entry: &SlotEntry) {
        let row = self.row(entry.slot);
        row.level = entry.level as u8;
        if let Some(block) = &entry.block {
            row.flags |= HAS_BLOCK;
            // Votes are what is left of the block once the rest is taken out,
            // the same reading the schedule page makes. Saturating because the
            // two counters are differenced independently and a bank whose
            // parent has gone reports neither.
            row.votes = clamp(
                block
                    .transactions
                    .saturating_sub(block.non_vote_transactions),
            );
            row.non_votes = clamp(block.non_vote_transactions);
            row.compute = clamp(block.block_cost);
            row.fees = block.total_fees;
        }
    }

    /// When the slot's first shred arrived, which the collector reads from the
    /// blockstore to difference the slot durations and otherwise discards.
    pub fn record_time(&mut self, slot: Slot, millis: u64) {
        let row = self.row(slot);
        row.flags |= HAS_CLOCK;
        row.time_millis = millis;
    }

    /// The row for `slot`, cleared first if the one in that position belongs to
    /// an older slot. Both writers reach a row through here, so neither can
    /// leave the other's fields behind from a previous lap.
    fn row(&mut self, slot: Slot) -> &mut PackedSlot {
        let index = self.index(slot);
        let held = &mut self.rows[index];
        if held.0 != slot {
            *held = (slot, PackedSlot::default());
        }
        &mut held.1
    }

    /// `slot % capacity`, without a bare remainder: the workspace denies
    /// `arithmetic_side_effects`, and the capacity being non-zero is a property
    /// of the constructor rather than of the type.
    fn index(&self, slot: Slot) -> usize {
        let capacity = self.rows.len() as u64;
        usize::try_from(slot.checked_rem(capacity).unwrap_or(0)).unwrap_or(0)
    }
}

/// Into `u32`, clamped rather than wrapped. Every counter this is used on is
/// far inside the range today, and a clamp reads as "at least this much" where
/// a wrap reads as a small number.
fn clamp(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::slots::{BlockDetail, SlotLevel},
    };

    fn entry(slot: Slot) -> SlotEntry {
        SlotEntry {
            slot,
            level: SlotLevel::Rooted,
            leader: None,
            leader_name: None,
            leader_icon: None,
            mine: false,
            block: None,
            duration_nanos: None,
        }
    }

    fn with_block(slot: Slot, transactions: u64, non_vote: u64) -> SlotEntry {
        SlotEntry {
            block: Some(BlockDetail {
                transactions,
                non_vote_transactions: non_vote,
                failed_transactions: 0,
                entries: 0,
                block_cost: 41_827_311,
                block_cost_limit: 60_000_000,
                account_cost_limit: 12_000_000,
                total_fees: 104_600_000,
                priority_fees: 0,
            }),
            ..entry(slot)
        }
    }

    #[test]
    fn test_a_slot_reads_back_what_was_recorded_for_it() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(900, 9_500, 8_752));
        history.record_time(900, 1_756_000_000_123);

        let row = history.get(900).expect("recorded");
        assert_eq!(row.votes, 748);
        assert_eq!(row.non_votes, 8_752);
        assert_eq!(row.compute, 41_827_311);
        assert_eq!(row.fees, 104_600_000);
        assert_eq!(row.time_millis, 1_756_000_000_123);
        assert_eq!(row.flags, HAS_BLOCK | HAS_CLOCK);
    }

    #[test]
    fn test_the_two_writers_do_not_clear_each_other() {
        // The block arrives when the bank freezes and the clock when the
        // blockstore is walked, on different ticks and in either order.
        for clock_first in [true, false] {
            let mut history = SlotHistory::new(64);
            if clock_first {
                history.record_time(900, 42);
                history.record(&with_block(900, 10, 4));
            } else {
                history.record(&with_block(900, 10, 4));
                history.record_time(900, 42);
            }
            let row = history.get(900).expect("recorded");
            assert_eq!(row.time_millis, 42, "clock_first={clock_first}");
            assert_eq!(row.non_votes, 4, "clock_first={clock_first}");
            assert_eq!(row.flags, HAS_BLOCK | HAS_CLOCK);
        }
    }

    #[test]
    fn test_a_slot_a_lap_ago_is_not_mistaken_for_this_one() {
        // The bug this shape invites. Both slots land in the same row, and
        // without the slot stored beside it the older one would answer for the
        // newer with a whole lap of stale figures.
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 5_000, 4_000));
        history.record(&with_block(74, 9, 4));

        assert!(history.get(10).is_none());
        assert_eq!(history.get(74).expect("recorded").non_votes, 4);
    }

    #[test]
    fn test_a_lapped_row_keeps_nothing_of_the_slot_it_held() {
        // Cleared rather than overwritten field by field: the new slot may have
        // no block yet, and the old one's counts must not answer for it.
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 5_000, 4_000));
        history.record_time(10, 99);
        history.record(&entry(74));

        let row = history.get(74).expect("recorded");
        assert_eq!(row.flags, 0);
        assert_eq!(row.non_votes, 0);
        assert_eq!(row.time_millis, 0);
    }

    #[test]
    fn test_a_slot_never_recorded_reads_as_absent() {
        let history = SlotHistory::new(64);
        assert!(history.get(900).is_none());
    }

    #[test]
    fn test_a_block_that_landed_empty_is_not_a_block_that_was_never_seen() {
        // Both are nought in every count, which is why the flag exists.
        let mut history = SlotHistory::new(64);
        history.record(&with_block(900, 0, 0));
        history.record(&entry(901));

        assert_eq!(
            history.get(900).expect("recorded").flags & HAS_BLOCK,
            HAS_BLOCK
        );
        assert_eq!(history.get(901).expect("recorded").flags & HAS_BLOCK, 0);
    }

    #[test]
    fn test_a_count_past_the_row_is_clamped_rather_than_wrapped() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(900, u64::MAX, 0));
        assert_eq!(history.get(900).expect("recorded").votes, u32::MAX);
    }
}
