/**
 * Holding a reading position in a list that changes above it.
 *
 * Split from the component so the decision can be tested without a browser,
 * which matters more than usual here: the browser feature this stands in for —
 * scroll anchoring — fires for one of the two lists on this page and not the
 * other, and is absent in Safari, so what happens cannot be read off either.
 */

/**
 * Where a scroller should sit after the measure it is read against moved from
 * `was` to `now`.
 *
 * That measure is the list's height where the live edge is the top of it, and
 * the offset of an anchor element where it is not — the schedule is read
 * against the boundary between produced and scheduled, which has a list either
 * side. Following the move keeps whatever is on screen where it was, which is
 * what a newest-first list needs: without it every arriving slot pushes the
 * rows being read down, two and a half times a second.
 *
 * Anchored, the measure moves both ways, and both are followed. Turns cross the
 * boundary from above, the far end of the schedule is trimmed behind them, and
 * a leader's details arriving late can change a row's height — all of it above
 * what is being read, and none of it a reason for the page to move.
 *
 * Three cases are left alone:
 *
 * - A scroller at the top, where that is the live edge. Sitting there is a
 *   request to see what arrives, not to be held away from it. `holdAtTop` says
 *   the live edge is elsewhere, and then the top is held like anywhere else.
 * - A measure that did not move.
 * - A scroller something else has already moved, which is the important one.
 *   Where the browser does anchor, it has already followed by the time this is
 *   asked, and following again would send the list twice as far.
 */
export function heldScrollTop(
  scrollTop: number,
  previousTop: number,
  was: number,
  now: number,
  holdAtTop = false,
): number {
  if (previousTop <= 0 && !holdAtTop) return scrollTop;
  if (scrollTop !== previousTop) return scrollTop;
  const moved = now - was;
  if (moved === 0) return scrollTop;
  // A height only ever grows here — slots pruned from the bottom shorten the
  // list without moving anything above them — so a shrink is not ours to chase.
  if (!holdAtTop && moved < 0) return scrollTop;
  return Math.max(0, scrollTop + moved);
}
