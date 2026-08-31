/**
 * Slots fetched from the validator's packed history, rather than pushed.
 *
 * The live socket carries whole slot entries for the few hundred slots a client
 * is sent on connect. Everything older is held on the validator in a much
 * cheaper form and asked for a span at a time. This turns a span of that back
 * into the entries the schedule page already knows how to draw, so nothing
 * downstream has to know where a turn came from.
 *
 * What comes back is a reconstruction, not the original. The packed row carries
 * the six figures a schedule row shows and no others, so the fields outside
 * that read as nought here: failed transactions, entries and priority fees are
 * absent rather than zero, and anything drawing them from these entries would
 * be drawing a number that was never measured.
 */

import { SLOTS_PER_TURN } from "./schedule";
import type { EpochInfo, SlotEntry, SlotLevel } from "./types";

/** Set where the slot recorded a block. */
export const HAS_BLOCK = 1;
/** Set where the slot's first shred was timed. */
export const HAS_CLOCK = 1 << 1;

/**
 * One slot as the validator sends it: positional, not an object.
 *
 * Order: level, flags, votes, non-votes, compute, fees, time. It is pinned by a
 * test here and by another on the validator, because two positional formats
 * only agree by being changed together.
 */
export type WireRow = [
  level: number,
  flags: number,
  votes: number,
  nonVotes: number,
  compute: number,
  fees: number,
  timeMillis: number,
];

/** A span of history, oldest first, with `null` for slots it does not hold. */
export interface SlotRange {
  first_slot: number;
  rows: (WireRow | null)[];
}

/**
 * Levels by their discriminant, in the order the validator's enum declares
 * them. The wire carries the number; this is the only place that knows which
 * name it stands for.
 */
const LEVELS: SlotLevel[] = [
  "incomplete",
  "completed",
  "optimistically_confirmed",
  "rooted",
  "finalized",
  "skipped",
];

/** What a leader is called and looks like, as the live slots reported it. */
export interface LeaderDisplay {
  name: string | null;
  icon: string | null;
}

/**
 * Who leads a slot, from the epoch's turn array.
 *
 * The array holds one index per run of consecutive slots, so this is two
 * lookups and no search. Null outside the epoch the arrays describe, and null
 * where the validator could not derive the schedule, which it sends as an empty
 * array rather than as a wrong one.
 */
export function leaderAt(epoch: EpochInfo | undefined, slot: number): string | null {
  if (!epoch || epoch.turns.length === 0) return null;
  if (slot < epoch.start_slot || slot > epoch.end_slot) return null;
  const turn = Math.floor((slot - epoch.start_slot) / SLOTS_PER_TURN);
  const index = epoch.turns[turn];
  if (index === undefined) return null;
  return epoch.leaders[index] ?? null;
}

/**
 * Names and icons for the leaders the live window has already reported.
 *
 * The epoch arrays carry keys and nothing else, on purpose: names and icons are
 * the largest strings a slot used to repeat, and sending them per epoch would
 * put them straight back. So a fetched turn is named from whatever the live
 * slots have taught the page, and falls back to its key where they have not.
 */
export function leaderDisplays(held: SlotEntry[]): Map<string, LeaderDisplay> {
  const displays = new Map<string, LeaderDisplay>();
  for (const entry of held) {
    if (entry.leader === null || displays.has(entry.leader)) continue;
    if (entry.leader_name === null && entry.leader_icon === null) continue;
    displays.set(entry.leader, { name: entry.leader_name, icon: entry.leader_icon });
  }
  return displays;
}

/**
 * A fetched span as slot entries, oldest first.
 *
 * Holes are dropped rather than turned into empty entries. A slot the validator
 * has no row for is one it never saw or has since aged out, and `turnsOf` draws
 * the gap on its own from the slots either side.
 */
export function entriesOf(
  range: SlotRange,
  epoch: EpochInfo | undefined,
  displays: Map<string, LeaderDisplay>,
  identity: string | undefined,
): SlotEntry[] {
  const entries: SlotEntry[] = [];
  // The gap to the previous slot that had a clock, which is what the validator
  // measures a duration as. Carried across holes for the same reason it is
  // there: a skipped slot shows up as one long interval, not as none.
  let previousTime: number | null = null;

  range.rows.forEach((row, index) => {
    if (row === null) return;
    const slot = range.first_slot + index;
    const [level, flags, votes, nonVotes, compute, fees, timeMillis] = row;
    const leader = leaderAt(epoch, slot);
    const display = leader === null ? undefined : displays.get(leader);
    const timed = (flags & HAS_CLOCK) !== 0;

    entries.push({
      slot,
      level: LEVELS[level] ?? "incomplete",
      leader,
      leader_name: display?.name ?? null,
      leader_icon: display?.icon ?? null,
      mine: leader !== null && leader === identity,
      block:
        (flags & HAS_BLOCK) === 0
          ? null
          : {
              transactions: votes + nonVotes,
              non_vote_transactions: nonVotes,
              // Not carried by the packed row. Nought here means "not
              // measured", and no schedule row reads them.
              failed_transactions: 0,
              entries: 0,
              block_cost: compute,
              block_cost_limit: epoch?.block_cost_limit ?? 0,
              account_cost_limit: epoch?.account_cost_limit ?? 0,
              total_fees: fees,
              priority_fees: 0,
            },
      duration_nanos:
        timed && previousTime !== null ? (timeMillis - previousTime) * 1_000_000 : null,
    });

    if (timed) previousTime = timeMillis;
  });

  return entries;
}
