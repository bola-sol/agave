//! A flat history of what each recent slot contained: fixed-size rows with
//! only the columns the schedule page draws, an epoch deep.

use {
    crate::{
        certs::Reward,
        slots::{SlotEntry, SlotLevel},
    },
    serde::Serialize,
    solana_clock::Slot,
};

/// An epoch, two days at 400 ms, in thirty-eight megabytes, allocated by the service since the
/// server answers range queries before the collector exists.
pub const PACKED_SLOTS: usize = 432_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PackedSlot {
    pub level: u8,
    pub flags: u16,
    pub votes: u32,
    pub non_votes: u32,
    /// Saturating into `u32`, seventy times the current block limit.
    pub compute: u32,
    /// In lamports; base is `fees - priority_fees`.
    pub fees: u64,
    pub priority_fees: u64,
    pub tips: u64,
    pub replay_micros: u32,
    pub time_millis: u64,
    pub shreds: u32,
    pub repaired: u32,
    pub full_millis: u32,
    pub replayed_millis: u32,
    pub left_out: u16,
    /// Saturating into `u16`, which keeps the row at its size.
    pub recovered: u16,
    pub failed: u32,
    /// Basis points of stake in the notarize reward certificate a later footer carried.
    pub notarized: u16,
}

/// A full span of mainnet-sized rows is under half the frame ceiling; a test holds it there.
pub const MAX_RANGE_SLOTS: usize = 4096;

/// One slot on the wire, a JSON array: level, flags, votes, non-votes, compute, fees, priority
/// fees, tips, time, replay, shreds, repaired, full, replayed, left out, recovered, failed. The
/// frontend mirrors it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WireRow(
    pub u8,
    pub u16,
    pub u32,
    pub u32,
    pub u32,
    pub u64,
    pub u64,
    pub u64,
    pub u64,
    pub u32,
    pub u32,
    pub u32,
    pub u32,
    pub u32,
    pub u16,
    pub u16,
    pub u32,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotRange {
    /// Every row after it is one slot on, so slot numbers are never sent.
    pub first_slot: Slot,
    pub rows: Vec<Option<WireRow>>,
}

pub const HAS_BLOCK: u16 = 1;
pub const HAS_CLOCK: u16 = 1 << 1;
/// Nought is a real reading: the searchers passed that leader by.
pub const HAS_TIPS: u16 = 1 << 2;
pub const HAS_REPLAY: u16 = 1 << 3;
pub const HAS_SHREDS: u16 = 1 << 4;
pub const HAS_REPLAYED: u16 = 1 << 5;
/// Two bits for the reward certificate's verdict on this node's vote: unseen,
/// paid, unpaid, or none because no rooted block carried one.
pub const REWARD_SHIFT: u16 = 6;
pub const REWARD_MASK: u16 = 0b11 << REWARD_SHIFT;
pub const REWARD_PAID: u16 = 1 << REWARD_SHIFT;
pub const REWARD_UNPAID: u16 = 2 << REWARD_SHIFT;
pub const REWARD_NONE: u16 = 3 << REWARD_SHIFT;
pub const HAS_NOTARIZED: u16 = 1 << 8;
/// The block's footer carried a finalization certificate, the fast one where `FAST_FINAL` is set.
pub const HAS_FINAL_CERT: u16 = 1 << 9;
pub const FAST_FINAL: u16 = 1 << 10;
/// Read back from the recording at startup, and not yet written by this run's own collector.
pub const READ_BACK: u16 = 1 << 11;
/// Written while its run was catching up, so its replay measures the backlog.
pub const CATCH_UP: u16 = 1 << 12;
/// The server's own marks, never sent.
const UNSENT: u16 = READ_BACK | CATCH_UP;
/// What a footer gives a slot, which the recording does not carry.
const FOOTER: u16 = HAS_NOTARIZED | HAS_FINAL_CERT | FAST_FINAL;
/// What a slot the cluster skipped cannot have.
const BLOCK_PARTS: u16 = HAS_BLOCK | HAS_TIPS | HAS_REPLAY | HAS_REPLAYED;

fn reward_bits(reward: Option<Reward>) -> u16 {
    match reward {
        None => 0,
        Some(Reward::Paid) => REWARD_PAID,
        Some(Reward::Unpaid) => REWARD_UNPAID,
        Some(Reward::NoCertificate) => REWARD_NONE,
    }
}

/// The slot is kept beside its row so a row from a lap ago cannot answer for a current one.
pub struct SlotHistory {
    rows: Vec<(Slot, PackedSlot)>,
    /// Slots below this were replayed while catching up after a start.
    live_from: Slot,
}

impl SlotHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            rows: vec![(0, PackedSlot::default()); capacity.max(1)],
            live_from: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.rows.len()
    }

    pub fn live_from(&self) -> Slot {
        self.live_from
    }

    pub fn set_live_from(&mut self, slot: Slot) {
        self.live_from = slot;
    }

    pub fn get(&self, slot: Slot) -> Option<&PackedSlot> {
        let (held, row) = self.rows.get(self.index(slot))?;
        // Unfilled rows hold slot 0, so slot 0 itself is never reported.
        (*held == slot && slot != 0).then_some(row)
    }

    pub fn range(&self, first_slot: Slot, count: usize) -> SlotRange {
        let count = count.min(MAX_RANGE_SLOTS);
        let rows = (0..count as u64)
            .map(|offset| {
                self.get(first_slot.saturating_add(offset)).map(|row| {
                    WireRow(
                        row.level,
                        row.flags & !UNSENT,
                        row.votes,
                        row.non_votes,
                        row.compute,
                        row.fees,
                        row.priority_fees,
                        row.tips,
                        row.time_millis,
                        row.replay_micros,
                        row.shreds,
                        row.repaired,
                        row.full_millis,
                        row.replayed_millis,
                        row.left_out,
                        row.recovered,
                        row.failed,
                    )
                })
            })
            .collect();
        SlotRange { first_slot, rows }
    }

    /// A row read back from the recording gives way to this run's only once this run has the
    /// block; before then, a line without one leaves it, as a restart writes such lines at its seam.
    pub fn record(&mut self, entry: &SlotEntry) {
        let index = self.index(entry.slot);
        if let Some((held, row)) = self.rows.get_mut(index)
            && *held == entry.slot
            && row.flags & READ_BACK != 0
        {
            if entry.block.is_none() {
                return;
            }
            *row = PackedSlot::default();
        }
        self.write(entry);
    }

    fn write(&mut self, entry: &SlotEntry) {
        let row = self.row(entry.slot);
        row.level = entry.level as u8;
        if let Some(block) = &entry.block {
            row.flags |= HAS_BLOCK;
            row.votes = clamp(
                block
                    .transactions
                    .saturating_sub(block.non_vote_transactions),
            );
            row.non_votes = clamp(block.non_vote_transactions);
            row.failed = clamp(block.failed_transactions);
            row.compute = clamp(block.block_cost);
            row.fees = block.total_fees;
            row.priority_fees = block.priority_fees;
            if let Some(tips) = block.tips {
                row.flags |= HAS_TIPS;
                row.tips = tips;
            }
            if let Some(micros) = block.replay_micros {
                row.flags |= HAS_REPLAY;
                row.replay_micros = clamp(micros);
            }
        }
        if let Some(shreds) = &entry.shreds {
            row.flags |= HAS_SHREDS;
            row.shreds = clamp(shreds.count);
            row.repaired = clamp(shreds.repaired);
            row.recovered = u16::try_from(shreds.recovered).unwrap_or(u16::MAX);
            row.full_millis = clamp(shreds.full_millis);
        }
        if let Some(millis) = entry.replayed_millis {
            row.flags |= HAS_REPLAYED;
            row.replayed_millis = clamp(millis);
        }
        row.flags = (row.flags & !REWARD_MASK) | reward_bits(entry.reward);
        row.left_out = entry.left_out.unwrap_or(0);
    }

    /// Takes each slot `older` holds into cells from `from`, `count` at a time so a reader is not
    /// held up for the whole ring. A slot this run has the block of keeps it, a newer lap keeps
    /// its cell, and a slot this run holds without a block takes the recorded row with this run's
    /// footer marks. Returns how many rows it took.
    pub fn merge_read_back(&mut self, older: &SlotHistory, from: usize, count: usize) -> usize {
        if older.rows.len() != self.rows.len() {
            return 0;
        }
        let mut taken = 0usize;
        let end = from.saturating_add(count).min(self.rows.len());
        for index in from..end {
            let (Some(&(slot, theirs)), Some(cell)) =
                (older.rows.get(index), self.rows.get_mut(index))
            else {
                continue;
            };
            if slot == 0 {
                continue;
            }
            let (held, ours) = cell;
            if *held > slot || (*held == slot && ours.flags & HAS_BLOCK != 0) {
                continue;
            }
            let mut row = theirs;
            row.flags |= READ_BACK;
            if *held == slot {
                row.flags = (row.flags & !FOOTER) | (ours.flags & FOOTER);
                row.notarized = ours.notarized;
            }
            *cell = (slot, row);
            taken = taken.saturating_add(1);
        }
        taken
    }

    /// Sets a read-back row's level once the ledger has judged it; a skipped slot loses its block.
    pub fn settle(&mut self, slot: Slot, level: SlotLevel) {
        let index = self.index(slot);
        let Some((held, row)) = self.rows.get_mut(index) else {
            return;
        };
        if *held != slot || row.flags & READ_BACK == 0 || row.level >= SlotLevel::Rooted as u8 {
            return;
        }
        row.level = level as u8;
        if level == SlotLevel::Skipped {
            row.flags &= !BLOCK_PARTS;
        }
    }

    /// Whether a row's replay measures replay, not a backlog after a start.
    pub fn replay_counts(&self, slot: Slot, row: &PackedSlot) -> bool {
        if row.flags & READ_BACK != 0 {
            return row.flags & CATCH_UP == 0;
        }
        slot >= self.live_from
    }

    /// The slots of read-back rows the ledger has yet to judge, newest first, `cap` at most.
    pub fn unsettled(&self, cap: usize) -> Vec<Slot> {
        let mut slots: Vec<Slot> = self
            .rows
            .iter()
            .filter(|(slot, row)| {
                *slot != 0 && row.flags & READ_BACK != 0 && row.level < SlotLevel::Rooted as u8
            })
            .map(|(slot, _)| *slot)
            .collect();
        slots.sort_unstable_by(|a, b| b.cmp(a));
        slots.truncate(cap);
        slots
    }

    /// One row written directly, for tests elsewhere in the crate.
    #[cfg(test)]
    pub(crate) fn record_test_row(&mut self, slot: Slot, fill: impl FnOnce(&mut PackedSlot)) {
        fill(self.row(slot));
    }

    pub fn record_notarized(&mut self, slot: Slot, basis_points: u16) {
        let row = self.row(slot);
        row.flags |= HAS_NOTARIZED;
        row.notarized = basis_points;
    }

    pub fn record_final_path(&mut self, slot: Slot, fast: bool) {
        let row = self.row(slot);
        row.flags = (row.flags & !FAST_FINAL) | HAS_FINAL_CERT | if fast { FAST_FINAL } else { 0 };
    }

    pub fn record_time(&mut self, slot: Slot, millis: u64) {
        let row = self.row(slot);
        row.flags |= HAS_CLOCK;
        row.time_millis = millis;
    }

    /// Every figure as large as a real slot's gets, for the tests that bound a reply's size.
    #[cfg(test)]
    pub(crate) fn record_worst_case(&mut self, slot: Slot) {
        *self.row(slot) = PackedSlot {
            level: 4,
            flags: HAS_BLOCK | HAS_CLOCK | HAS_TIPS | HAS_REPLAY | HAS_SHREDS | HAS_REPLAYED,
            votes: 99_999,
            non_votes: 99_999,
            compute: u32::MAX,
            fees: 9_999_999_999_999,
            priority_fees: 9_999_999_999_999,
            tips: 9_999_999_999_999,
            replay_micros: 9_999_999,
            time_millis: 1_999_999_999_999,
            shreds: 99_999,
            repaired: 99_999,
            full_millis: 999_999,
            replayed_millis: 999_999,
            left_out: 9_999,
            recovered: 9_999,
            failed: 99_999,
            notarized: 10_000,
        };
    }

    fn row(&mut self, slot: Slot) -> &mut PackedSlot {
        let index = self.index(slot);
        let held = &mut self.rows[index];
        if held.0 != slot {
            *held = (slot, PackedSlot::default());
        }
        &mut held.1
    }

    /// Without a bare remainder: the workspace denies `arithmetic_side_effects`.
    fn index(&self, slot: Slot) -> usize {
        let capacity = self.rows.len() as u64;
        usize::try_from(slot.checked_rem(capacity).unwrap_or(0)).unwrap_or(0)
    }
}

/// The rows a recording reads back as, built apart from the history in use and merged into it once
/// read. Each row remembers the run of the process that wrote it last.
pub struct ReadBack {
    history: SlotHistory,
    runs: Vec<u16>,
}

impl ReadBack {
    pub fn new(capacity: usize) -> Self {
        let history = SlotHistory::new(capacity);
        let runs = vec![0; history.rows.len()];
        Self { history, runs }
    }

    pub fn history(&self) -> &SlotHistory {
        &self.history
    }

    /// One recorded entry from run `run`. Within a run nothing regresses, as a segment's opening
    /// copy can be older than the lines before it; across runs, a line without a block leaves a
    /// row that has one, and a line with a block starts the row afresh.
    pub fn apply(&mut self, entry: &SlotEntry, run: u16, catching_up: bool) {
        let index = self.history.index(entry.slot);
        let before = self.history.get(entry.slot).copied();
        let same_run = self.runs.get(index) == Some(&run);
        let mut kept = None;
        match before {
            Some(row) if !same_run && entry.block.is_none() && row.flags & HAS_BLOCK != 0 => return,
            Some(_) if !same_run => {
                if let Some(cell) = self.history.rows.get_mut(index) {
                    cell.1 = PackedSlot::default();
                }
            }
            Some(row) => kept = Some(row),
            None => {}
        }
        self.history.write(entry);
        if let Some(millis) = entry.time_millis {
            self.history.record_time(entry.slot, millis);
        }
        let row = self.history.row(entry.slot);
        if let Some(before) = kept {
            row.level = row.level.max(before.level);
            if entry.reward.is_none() {
                row.flags = (row.flags & !REWARD_MASK) | (before.flags & REWARD_MASK);
            }
            if entry.left_out.is_none() {
                row.left_out = before.left_out;
            }
            row.flags |= before.flags & CATCH_UP;
        }
        if catching_up {
            row.flags |= CATCH_UP;
        }
        if let Some(held) = self.runs.get_mut(index) {
            *held = run;
        }
    }
}

fn clamp(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::slots::{BlockDetail, ShredArrival, SlotLevel},
    };

    fn entry(slot: Slot) -> SlotEntry {
        SlotEntry {
            slot,
            level: SlotLevel::Rooted,
            mine: false,
            block: None,
            duration_nanos: None,
            time_millis: None,
            shreds: None,
            replayed_millis: None,
            reward: None,
            left_out: None,
        }
    }

    #[test]
    fn test_the_reward_verdict_packs_into_two_bits() {
        let mut history = SlotHistory::new(64);
        for (reward, bits) in [
            (Some(Reward::Paid), REWARD_PAID),
            (Some(Reward::Unpaid), REWARD_UNPAID),
            (Some(Reward::NoCertificate), REWARD_NONE),
            (None, 0),
        ] {
            let mut seen = entry(10);
            seen.reward = reward;
            history.record(&seen);
            assert_eq!(history.get(10).expect("recorded").flags & REWARD_MASK, bits);
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
                tips: None,
                replay_micros: None,
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
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 5_000, 4_000));
        history.record(&with_block(74, 9, 4));

        assert!(history.get(10).is_none());
        assert_eq!(history.get(74).expect("recorded").non_votes, 4);
    }

    #[test]
    fn test_a_lapped_row_keeps_nothing_of_the_slot_it_held() {
        // Cleared rather than overwritten, so the old slot's counts cannot answer for the new one.
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
    fn test_a_range_is_positional_and_holds_a_gap_open() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 10, 4));
        history.record(&with_block(12, 20, 9));

        let range = history.range(10, 3);
        assert_eq!(range.first_slot, 10);
        assert_eq!(range.rows.len(), 3);
        assert!(range.rows[0].is_some());
        assert!(range.rows[1].is_none(), "slot 11 was never recorded");
        assert_eq!(range.rows[2].expect("slot 12").3, 9);
    }

    #[test]
    fn test_a_range_past_the_ceiling_is_clamped_rather_than_refused() {
        let history = SlotHistory::new(64);
        let range = history.range(10, MAX_RANGE_SLOTS.saturating_add(1_000));
        assert_eq!(range.rows.len(), MAX_RANGE_SLOTS);
    }

    #[test]
    fn test_a_range_off_the_end_of_the_history_is_all_holes() {
        let history = SlotHistory::new(64);
        let range = history.range(900, 4);
        assert_eq!(range.rows.len(), 4);
        assert!(range.rows.iter().all(Option::is_none));
    }

    #[test]
    fn test_range_column_order() {
        // The one place the wire order is pinned.
        let mut history = SlotHistory::new(64);
        let mut block = with_block(10, 9_500, 8_752);
        if let Some(detail) = block.block.as_mut() {
            detail.failed_transactions = 37;
        }
        history.record(&block);
        history.record_time(10, 1_756_000_000_123);

        let WireRow(
            level,
            flags,
            votes,
            non_votes,
            compute,
            fees,
            priority,
            tips,
            time,
            replay,
            shreds,
            repaired,
            full,
            replayed,
            left_out,
            recovered,
            failed,
        ) = history.range(10, 1).rows[0].expect("recorded");
        assert_eq!(level, SlotLevel::Rooted as u8);
        assert_eq!(flags, HAS_BLOCK | HAS_CLOCK);
        assert_eq!(left_out, 0);
        assert_eq!(votes, 748);
        assert_eq!(non_votes, 8_752);
        assert_eq!(failed, 37);
        assert_eq!(compute, 41_827_311);
        assert_eq!(fees, 104_600_000);
        assert_eq!(priority, 0);
        assert_eq!(tips, 0);
        assert_eq!(flags & HAS_TIPS, 0);
        assert_eq!(time, 1_756_000_000_123);
        assert_eq!(replay, 0);
        assert_eq!(flags & HAS_REPLAY, 0);
        assert_eq!(
            (shreds, repaired, recovered, full, replayed),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(flags & (HAS_SHREDS | HAS_REPLAYED), 0);
    }

    #[test]
    fn test_footer_readings_set_their_flags_and_keep_the_rest() {
        let mut history = SlotHistory::new(16);
        history.record(&entry(5));
        history.record_notarized(5, 9_950);
        history.record_final_path(5, true);
        let row = *history.get(5).unwrap();
        assert_eq!(row.notarized, 9_950);
        assert_eq!(
            row.flags & (HAS_NOTARIZED | HAS_FINAL_CERT | FAST_FINAL),
            HAS_NOTARIZED | HAS_FINAL_CERT | FAST_FINAL
        );
        history.record_final_path(5, false);
        history.record(&entry(5));
        let row = *history.get(5).unwrap();
        assert_eq!(
            row.flags & (HAS_FINAL_CERT | FAST_FINAL),
            HAS_FINAL_CERT,
            "a later read replaces the path"
        );
        assert_eq!(
            row.flags & HAS_NOTARIZED,
            HAS_NOTARIZED,
            "a refreshed entry keeps the reading"
        );
    }

    #[test]
    fn test_a_held_row_stays_eighty_eight_bytes() {
        // 432,000 of them is the thirty-eight megabytes the ring is sized by.
        assert_eq!(std::mem::size_of::<(Slot, PackedSlot)>(), 88);
    }

    #[test]
    fn test_shred_arrival_and_replay_end_travel_with_their_flags() {
        let mut history = SlotHistory::new(64);
        let mut filled = entry(50);
        filled.shreds = Some(ShredArrival {
            count: 928,
            repaired: 63,
            recovered: 12,
            full_millis: 921,
        });
        history.record(&filled);
        let mut replayed = with_block(51, 100, 10);
        replayed.shreds = Some(ShredArrival {
            count: 1_203,
            repaired: 0,
            recovered: 70_000,
            full_millis: 341,
        });
        replayed.replayed_millis = Some(393);
        history.record(&replayed);

        let dead = history.get(50).expect("recorded");
        assert_eq!(dead.flags, HAS_SHREDS);
        assert_eq!(
            (dead.shreds, dead.repaired, dead.recovered, dead.full_millis),
            (928, 63, 12, 921)
        );
        assert_eq!(dead.replayed_millis, 0);

        let live = history.get(51).expect("recorded");
        assert_eq!(live.flags, HAS_BLOCK | HAS_SHREDS | HAS_REPLAYED);
        assert_eq!(live.recovered, u16::MAX);
        assert_eq!(live.full_millis, 341);
        assert_eq!(live.replayed_millis, 393);
    }

    #[test]
    fn test_full_range_fits_the_message_ceiling() {
        // Every figure as large as a real slot's gets; the worst case the types
        // allow does not fit and never did.
        let mut history = SlotHistory::new(MAX_RANGE_SLOTS);
        for slot in 0..MAX_RANGE_SLOTS as Slot {
            let mut big = with_block(slot, 99_999, 99_999);
            if let Some(block) = big.block.as_mut() {
                block.failed_transactions = 9_999;
                block.block_cost = u64::from(u32::MAX);
                block.total_fees = 9_999_999_999_999;
                block.priority_fees = 9_999_999_999_999;
                block.tips = Some(9_999_999_999_999);
                block.replay_micros = Some(9_999_999);
            }
            big.shreds = Some(ShredArrival {
                count: 99_999,
                repaired: 99_999,
                recovered: 9_999,
                full_millis: 999_999,
            });
            big.replayed_millis = Some(999_999);
            history.record(&big);
            history.record_time(slot, 1_999_999_999_999);
        }

        let encoded = crate::proto::encode("slot", "range", &history.range(0, MAX_RANGE_SLOTS));
        assert!(
            encoded.len() < crate::proto::MAX_MESSAGE / 2,
            "a full range is {} bytes against a {} byte ceiling",
            encoded.len(),
            crate::proto::MAX_MESSAGE
        );
    }

    #[test]
    fn test_replay_time_travels_with_its_flag() {
        let mut history = SlotHistory::new(64);
        let mut replayed = with_block(40, 100, 10);
        if let Some(block) = replayed.block.as_mut() {
            block.replay_micros = Some(47_200);
        }
        history.record(&replayed);
        history.record(&with_block(41, 100, 10));

        let read = history.get(40).expect("recorded");
        assert_eq!(read.flags & HAS_REPLAY, HAS_REPLAY);
        assert_eq!(read.replay_micros, 47_200);
        let built = history.get(41).expect("recorded");
        assert_eq!(built.flags & HAS_REPLAY, 0);
    }

    #[test]
    fn test_tips_of_nought_are_not_tips_that_were_never_read() {
        // A turn the searchers passed by and one never measured must not draw the same.
        let mut history = SlotHistory::new(64);

        let mut measured = with_block(20, 100, 10);
        if let Some(block) = measured.block.as_mut() {
            block.tips = Some(0);
        }
        history.record(&measured);
        history.record(&with_block(21, 100, 10));

        let read = history.get(20).expect("recorded");
        assert_eq!(read.flags & HAS_TIPS, HAS_TIPS);
        assert_eq!(read.tips, 0);

        let unread = history.get(21).expect("recorded");
        assert_eq!(unread.flags & HAS_TIPS, 0);
        assert_eq!(unread.tips, 0);
    }

    #[test]
    fn test_the_two_kinds_of_fee_are_kept_apart() {
        let mut history = SlotHistory::new(64);
        let mut block = with_block(30, 100, 10);
        if let Some(detail) = block.block.as_mut() {
            detail.total_fees = 104_600_000;
            detail.priority_fees = 99_000_000;
        }
        history.record(&block);

        let read = history.get(30).expect("recorded");
        assert_eq!(read.fees, 104_600_000);
        assert_eq!(read.priority_fees, 99_000_000);
        assert_eq!(read.fees.saturating_sub(read.priority_fees), 5_600_000);
    }

    #[test]
    fn test_empty_block_differs_from_no_block() {
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

    fn finalized(slot: Slot, fees: u64) -> SlotEntry {
        let mut seen = with_block(slot, 100, 40);
        seen.level = SlotLevel::Finalized;
        if let Some(block) = seen.block.as_mut() {
            block.total_fees = fees;
        }
        seen
    }

    fn read_back(entries: &[(SlotEntry, u16, bool)]) -> ReadBack {
        let mut fill = ReadBack::new(64);
        for (entry, run, catching_up) in entries {
            fill.apply(entry, *run, *catching_up);
        }
        fill
    }

    #[test]
    fn test_a_read_back_row_never_goes_back_within_its_run() {
        let mut rewarded = finalized(10, 500);
        rewarded.reward = Some(Reward::Paid);
        rewarded.left_out = Some(3);
        // A segment's opening copy, from a second before the last lines of the one before it.
        let mut stale = finalized(10, 500);
        stale.level = SlotLevel::Completed;
        let fill = read_back(&[(rewarded, 1, false), (stale, 1, false)]);
        let row = fill.history().get(10).unwrap();
        assert_eq!(row.level, SlotLevel::Finalized as u8);
        assert_eq!(row.flags & REWARD_MASK, REWARD_PAID);
        assert_eq!(row.left_out, 3);
    }

    #[test]
    fn test_a_later_run_takes_a_slot_only_with_its_block() {
        let mut seam = entry(10);
        seam.level = SlotLevel::Skipped;
        let fill = read_back(&[(finalized(10, 500), 1, false), (seam, 2, false)]);
        assert_eq!(fill.history().get(10).unwrap().fees, 500);
        let fill = read_back(&[
            (finalized(10, 500), 1, false),
            (finalized(10, 900), 2, true),
        ]);
        let row = fill.history().get(10).unwrap();
        assert_eq!(row.fees, 900);
        assert_ne!(row.flags & CATCH_UP, 0);
    }

    #[test]
    fn test_a_read_back_row_keeps_its_time() {
        let mut timed = finalized(10, 500);
        timed.time_millis = Some(1_764_000_000_000);
        let fill = read_back(&[(timed, 1, false)]);
        let row = fill.history().get(10).unwrap();
        assert_eq!(row.time_millis, 1_764_000_000_000);
        assert_ne!(row.flags & HAS_CLOCK, 0);
    }

    #[test]
    fn test_a_merge_fills_what_this_run_lacks_and_keeps_what_it_has() {
        let fill = read_back(&[
            (finalized(10, 500), 1, false),
            (finalized(11, 600), 1, false),
            (finalized(12, 700), 1, false),
        ]);
        let mut live = SlotHistory::new(64);
        live.record(&finalized(11, 9));
        live.record(&entry(12));
        live.record_notarized(12, 9_500);
        // A newer lap in slot 10's cell.
        live.record(&finalized(74, 1));
        assert_eq!(live.merge_read_back(fill.history(), 0, 64), 1);
        assert_eq!(live.get(11).unwrap().fees, 9);
        assert!(live.get(10).is_none());
        let twelve = live.get(12).unwrap();
        assert_eq!(twelve.fees, 700);
        assert_eq!(twelve.notarized, 9_500);
        assert_ne!(twelve.flags & HAS_NOTARIZED, 0);
    }

    #[test]
    fn test_a_merge_goes_a_chunk_at_a_time() {
        let fill = read_back(&[
            (finalized(10, 500), 1, false),
            (finalized(40, 600), 1, false),
        ]);
        let mut live = SlotHistory::new(64);
        assert_eq!(live.merge_read_back(fill.history(), 0, 32), 1);
        assert!(live.get(40).is_none());
        assert_eq!(live.merge_read_back(fill.history(), 32, 32), 1);
        assert_eq!(live.get(40).unwrap().fees, 600);
        assert_eq!(live.merge_read_back(&SlotHistory::new(32), 0, 64), 0);
    }

    #[test]
    fn test_this_run_replaces_a_read_back_row_only_once_it_has_the_block() {
        let fill = read_back(&[(finalized(10, 500), 1, true)]);
        let mut live = SlotHistory::new(64);
        live.merge_read_back(fill.history(), 0, 64);
        let mut seam = entry(10);
        seam.level = SlotLevel::Skipped;
        live.record(&seam);
        assert_eq!(live.get(10).unwrap().fees, 500);
        live.record(&finalized(10, 800));
        let row = live.get(10).unwrap();
        assert_eq!(row.fees, 800);
        assert_eq!(row.flags & (READ_BACK | CATCH_UP), 0);
    }

    #[test]
    fn test_the_read_back_marks_are_never_sent() {
        let fill = read_back(&[(finalized(10, 500), 1, true)]);
        let mut live = SlotHistory::new(64);
        live.merge_read_back(fill.history(), 0, 64);
        let flags = live.range(10, 1).rows[0].expect("held").1;
        assert_eq!(flags & (READ_BACK | CATCH_UP), 0);
        assert_ne!(flags & HAS_BLOCK, 0);
    }

    #[test]
    fn test_a_read_back_row_settles_once_and_a_skipped_one_loses_its_block() {
        let mut completed = finalized(10, 500);
        completed.level = SlotLevel::Completed;
        let mut also = finalized(11, 500);
        also.level = SlotLevel::Completed;
        let fill = read_back(&[
            (completed, 1, false),
            (also, 1, false),
            (finalized(12, 500), 1, false),
        ]);
        let mut live = SlotHistory::new(64);
        live.merge_read_back(fill.history(), 0, 64);
        assert_eq!(live.unsettled(10), [11, 10]);
        live.settle(10, SlotLevel::Rooted);
        live.settle(11, SlotLevel::Skipped);
        assert_eq!(live.get(10).unwrap().level, SlotLevel::Rooted as u8);
        let skipped = live.get(11).unwrap();
        assert_eq!(skipped.level, SlotLevel::Skipped as u8);
        assert_eq!(skipped.flags & HAS_BLOCK, 0);
        // A recorded rooted or finalized level is trusted.
        live.settle(12, SlotLevel::Skipped);
        assert_eq!(live.get(12).unwrap().level, SlotLevel::Finalized as u8);
        assert!(live.unsettled(10).is_empty());
    }

    #[test]
    fn test_replay_counts_by_the_run_that_read_a_row_back() {
        let mut timed = finalized(10, 500);
        if let Some(block) = timed.block.as_mut() {
            block.replay_micros = Some(8000);
        }
        let mut late = timed.clone();
        late.slot = 11;
        let fill = read_back(&[(timed, 1, false), (late, 1, true)]);
        let mut live = SlotHistory::new(64);
        live.set_live_from(Slot::MAX);
        live.merge_read_back(fill.history(), 0, 64);
        assert!(live.replay_counts(10, live.get(10).unwrap()));
        assert!(!live.replay_counts(11, live.get(11).unwrap()));
        live.record(&finalized(20, 1));
        assert!(!live.replay_counts(20, live.get(20).unwrap()));
        live.set_live_from(15);
        assert!(live.replay_counts(20, live.get(20).unwrap()));
    }
}
