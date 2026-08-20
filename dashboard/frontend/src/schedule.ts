/**
 * Folding the slot list into the leader groups the schedule page shows.
 *
 * The leader schedule hands each validator a run of consecutive slots, so a
 * list of slots is already a list of groups; this only has to find the seams.
 * Kept out of the component so it can be tested without a DOM, in the same way
 * as the bar scale and the chart windowing.
 */

import type { SlotEntry, UpcomingSlot } from "./types";

/**
 * Slots the leader schedule hands out at a time.
 *
 * A validator drawn twice in a row leads eight consecutive slots, and three
 * times, twelve. Those are separate turns and are drawn as separate cards: run
 * together they made cards of two and three times the height of every other,
 * and a page whose rows are all different heights has no fixed position to
 * hold. Firedancer draws them apart for the same reason.
 */
const SLOTS_PER_TURN = 4;

/** The parts of a slot that decide which group it belongs to. */
export interface Led {
  slot: number;
  leader: string | null;
  leader_name: string | null;
  leader_icon: string | null;
  mine: boolean;
}

export interface LeaderGroup<T extends Led> {
  leader: string | null;
  leader_name: string | null;
  leader_icon: string | null;
  mine: boolean;
  /** In the order they were given, which the page keeps newest first. */
  slots: T[];
}

/**
 * Consecutive slots with the same leader, one turn at a time, in the order
 * given.
 *
 * A gap in the slot numbers starts a new group even when the leader matches:
 * two separate runs by the same validator are two turns at leading, and drawing
 * them as one would claim a run that never happened. So does crossing one of
 * the schedule's own boundaries, which is what keeps every card the same size —
 * see [`SLOTS_PER_TURN`].
 */
export function groupByLeader<T extends Led>(rows: T[]): LeaderGroup<T>[] {
  const groups: LeaderGroup<T>[] = [];
  const turnOf = (slot: number) => Math.floor(slot / SLOTS_PER_TURN);

  for (const row of rows) {
    const open = groups.at(-1);
    const previous = open?.slots.at(-1);
    const adjacent =
      previous !== undefined &&
      Math.abs(previous.slot - row.slot) === 1 &&
      turnOf(previous.slot) === turnOf(row.slot);

    if (open && adjacent && open.leader === row.leader) {
      open.slots.push(row);
      continue;
    }
    groups.push({
      leader: row.leader,
      leader_name: row.leader_name,
      leader_icon: row.leader_icon,
      mine: row.mine,
      slots: [row],
    });
  }

  return groups;
}

/**
 * Whether a group answers a search.
 *
 * Matches the leader's name or key, or any slot number in the group, so that
 * pasting either a validator or a slot finds the same row. An empty query
 * matches everything rather than nothing.
 */
export function matchesQuery<T extends Led>(group: LeaderGroup<T>, query: string): boolean {
  const needle = query.trim().toLowerCase();
  if (!needle) return true;

  if (group.leader_name?.toLowerCase().includes(needle)) return true;
  if (group.leader?.toLowerCase().includes(needle)) return true;
  return group.slots.some((slot) => String(slot.slot).includes(needle));
}

/**
 * One row of the timeline: a slot that has begun, or one the schedule promises.
 *
 * Both come from different places and the page has to read them as one run, or
 * a leader's turn straddling the boundary appears twice — shrinking on one side
 * of it and growing on the other as its slots are produced.
 */
export interface TimelineSlot extends Led {
  /** What replay found, or `null` for a slot that has not begun. */
  entry: SlotEntry | null;
}

/**
 * The slots held and the ones scheduled after them, newest first.
 *
 * A slot in both is the held one: the store knows what happened, the schedule
 * only knows who was asked to.
 */
export function timeline(held: SlotEntry[], scheduled: UpcomingSlot[]): TimelineSlot[] {
  const known = new Set(held.map((entry) => entry.slot));
  const rows: TimelineSlot[] = [
    ...scheduled
      .filter((slot) => !known.has(slot.slot))
      .map((slot) => ({ ...slot, entry: null })),
    ...held.map((entry) => ({
      slot: entry.slot,
      leader: entry.leader,
      leader_name: entry.leader_name,
      leader_icon: entry.leader_icon,
      mine: entry.mine,
      entry,
    })),
  ];
  return rows.sort((a, b) => b.slot - a.slot);
}

/**
 * Whether a leader's turn has started, which is what decides the side of the
 * boundary it is drawn on.
 *
 * A turn moves across whole. Splitting it at the slot being produced is what
 * made the page restless: the group above lost a row and the group below
 * gained one, several times a leader, and everything after them moved. Drawn
 * whole, with its unstarted slots as empty rows waiting to be filled, a turn
 * keeps its height from the moment it appears.
 *
 * Groups are newest first, so the turn's own first slot is its last row.
 */
export function hasBegun(group: LeaderGroup<TimelineSlot>): boolean {
  return group.slots.at(-1)?.entry != null;
}

/** A heading, or one leader's turn. What the list renders, one per row. */
export type ScheduleRow =
  | { kind: "heading"; label: string }
  | { kind: "group"; group: LeaderGroup<TimelineSlot> };

/**
 * The page as a flat list of rows, newest first, with its two headings in place.
 *
 * Flat because the list is virtualised, and a virtualised list is addressed by
 * index: the headings have to be items of their own rather than markup wrapped
 * around slices of the data, or their positions could not be measured or
 * scrolled to.
 */
export function scheduleRows(groups: LeaderGroup<TimelineSlot>[]): ScheduleRow[] {
  const scheduled = groups.filter((group) => !hasBegun(group));
  const produced = groups.filter(hasBegun);
  const rows: ScheduleRow[] = [];

  if (scheduled.length > 0) rows.push({ kind: "heading", label: "Upcoming" });
  for (const group of scheduled) rows.push({ kind: "group", group });
  rows.push({ kind: "heading", label: "Produced" });
  for (const group of produced) rows.push({ kind: "group", group });

  return rows;
}

/**
 * A stable name for a row, which is what lets a changed list be compared with
 * the one before it.
 *
 * A turn is named by its own first slot rather than by its position: turns
 * arrive above it and fall off below it constantly, and a name that moved with
 * them would identify nothing.
 */
export function rowKey(row: ScheduleRow): string {
  return row.kind === "heading" ? `heading:${row.label}` : `turn:${row.group.slots.at(-1)?.slot}`;
}

/**
 * How many rows were added above `previous` to make `keys`.
 *
 * The virtualised list is told this rather than left to work it out, and it
 * shifts its own record of where everything sits by that much — which is how
 * the view stays put while turns arrive above it. Measuring pixels after the
 * fact, which is what this replaces, can only ever guess at it.
 *
 * A first row that is no longer in the list at all means the two have nothing
 * in common — a search was typed, or the connection dropped and came back — and
 * nothing can be held across that.
 */
export function prependedCount(previous: string[], keys: string[]): number {
  const first = previous[0];
  if (first === undefined) return 0;
  const at = keys.indexOf(first);
  return at > 0 ? at : 0;
}
