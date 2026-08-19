/**
 * Folding the slot list into the leader groups the schedule page shows.
 *
 * The leader schedule hands each validator a run of consecutive slots, so a
 * list of slots is already a list of groups; this only has to find the seams.
 * Kept out of the component so it can be tested without a DOM, in the same way
 * as the bar scale and the chart windowing.
 */

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
