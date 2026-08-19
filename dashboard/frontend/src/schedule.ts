/**
 * Folding the slot list into the leader groups the schedule page shows.
 *
 * The leader schedule hands each validator a run of consecutive slots, so a
 * list of slots is already a list of groups; this only has to find the seams.
 * Kept out of the component so it can be tested without a DOM, in the same way
 * as the bar scale and the chart windowing.
 */

import type { SlotEntry, UpcomingSlot } from "./types";

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
 * Consecutive slots with the same leader, in the order given.
 *
 * A gap in the slot numbers starts a new group even when the leader matches.
 * Two separate runs by the same validator are two turns at leading, and drawing
 * them as one would claim a run that never happened.
 */
export function groupByLeader<T extends Led>(rows: T[]): LeaderGroup<T>[] {
  const groups: LeaderGroup<T>[] = [];

  for (const row of rows) {
    const open = groups.at(-1);
    const previous = open?.slots.at(-1);
    const adjacent = previous !== undefined && Math.abs(previous.slot - row.slot) === 1;

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
