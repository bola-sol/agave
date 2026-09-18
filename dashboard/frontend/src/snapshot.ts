/** The Status card's snapshot line: the newest archive, its age and the full
 *  it sits on, with the next ones due on the hover. */

import { count, duration } from "./format";
import type { Snapshots } from "./types";

export interface SnapshotLine {
  text: string;
  /** One sentence naming when the next archives are due, where the intervals
   *  and the slot rate are known. */
  title: string | undefined;
}

/** Blocks until the next multiple of `interval` above `height`, which is
 *  where the validator takes the next one. */
export function blocksUntil(height: number, interval: number): number {
  return interval - (height % interval);
}

/** Null where no archive is on disk. Ages are read against the validator's
 *  clock, which arrives every second. */
export function snapshotLine(
  snapshots: Snapshots,
  nowMillis: number | undefined,
  blockHeight: number | undefined,
  slotMillis: number | undefined,
): SnapshotLine | null {
  const newest = snapshots.incremental ?? snapshots.full;
  if (!newest) return null;
  const parts = [`snapshot ${count(newest.slot)}`];
  if (nowMillis !== undefined && newest.written_millis !== null) {
    parts.push(`${duration(Math.max(0, nowMillis - newest.written_millis))} ago`);
  }
  if (snapshots.incremental && snapshots.full) parts.push(`full ${count(snapshots.full.slot)}`);
  return { text: parts.join(" · "), title: nextDue(snapshots, blockHeight, slotMillis) };
}

function nextDue(
  snapshots: Snapshots,
  blockHeight: number | undefined,
  slotMillis: number | undefined,
): string | undefined {
  if (blockHeight === undefined || slotMillis === undefined) return undefined;
  const due = (kind: string, interval: number | null): string | null =>
    interval === null
      ? null
      : `next ${kind} in about ${duration(blocksUntil(blockHeight, interval) * slotMillis)}`;
  const clauses = [due("incremental", snapshots.incremental_interval), due("full", snapshots.full_interval)]
    .filter((clause): clause is string => clause !== null);
  if (clauses.length === 0) return undefined;
  const sentence = clauses.join(", ");
  return `${sentence.charAt(0).toUpperCase()}${sentence.slice(1)}.`;
}
