/**
 * Holding a reading position in a list that grows at the top.
 *
 * Split from the component so the decision can be tested without a browser,
 * which matters more than usual here: the browser feature this stands in for —
 * scroll anchoring — fires for one of the two lists on this page and not the
 * other, and is absent in Safari, so what happens cannot be read off either.
 */

/**
 * Where a scroller should sit after growing from `was` to `now` tall.
 *
 * Adding the growth keeps whatever is on screen where it was, which is what a
 * newest-first list needs: without it every arriving slot pushes the rows being
 * read down, two and a half times a second.
 *
 * Three cases are left alone:
 *
 * - A scroller at the live edge. Sitting at the top is a request to see what
 *   arrives, not to be held away from it.
 * - A list that did not grow. Slots are pruned from the bottom, which moves
 *   nothing above them.
 * - A scroller something else has already moved, which is the important one.
 *   Where the browser does anchor, it has already added the growth by the time
 *   this is asked, and adding it again would send the list twice as far as it
 *   should go.
 */
export function heldScrollTop(
  scrollTop: number,
  previousTop: number,
  was: number,
  now: number,
): number {
  if (previousTop <= 0) return scrollTop;
  if (scrollTop !== previousTop) return scrollTop;
  const grown = now - was;
  return grown > 0 ? scrollTop + grown : scrollTop;
}
