import { useEffect, useLayoutEffect, useRef, useState, type RefObject } from "react";
import { heldScrollTop } from "../scroll";

/**
 * How far a list must be scrolled before the way back is offered.
 *
 * A few rows, so that nudging it does not put a button over it, and so that the
 * button is gone whenever the live edge is already on screen.
 */
const LIVE_EDGE_PX = 120;

/**
 * Where `target` sits in `scroller`'s content, in pixels from its top.
 *
 * Measured through the viewport rather than by `offsetTop`, which is relative
 * to the nearest positioned ancestor and so only agrees between the two when
 * they happen to share one. They do today; a `position` on anything between
 * them would have broken it silently.
 */
function contentTop(scroller: HTMLElement, target: HTMLElement): number {
  const delta = target.getBoundingClientRect().top - scroller.getBoundingClientRect().top;
  return scroller.scrollTop + delta;
}

/**
 * A pill that returns a scrolled list to its live edge.
 *
 * `live` names the element the list is read against — for the schedule, the
 * boundary between what has been produced and what is still to come — and
 * `liveOffset` how far above it to sit, so that a little of what is coming
 * shows. Without one, the live edge is the top of the scroller.
 *
 * That element is also what the list is held against as it changes. Holding a
 * height catches only growth; holding an element catches anything that moves
 * it, and above the boundary that is a great deal — turns crossing from one
 * side to the other, the far end of the schedule trimmed, leader details
 * arriving late and changing a row's height.
 *
 * Both lists are newest-first and hundreds of rows long, so scrolling back by
 * hand is a long way. Takes the scroller rather than finding one, because the
 * two live in different parts of the tree.
 *
 * The wrapper carries no height, so the button hangs over the rows instead of
 * moving them, and is sticky rather than absolute so it tracks the list it
 * belongs to and not the page.
 */
export function ScrollTop({
  scroller,
  live,
  liveOffset = 0,
}: {
  scroller: RefObject<HTMLElement | null>;
  live?: RefObject<HTMLElement | null>;
  liveOffset?: number;
}) {
  const [away, setAway] = useState(false);
  // Shared with the hook below: it needs to know where the list was left, to
  // tell its own correction apart from one the browser already made.
  const top = useRef(0);
  useHeldScroll(scroller, top, live);

  useEffect(() => {
    const element = scroller.current;
    if (!element) return;

    const liveTop = () => {
      const target = live?.current;
      if (!target) return 0;
      return Math.max(0, contentTop(element, target) - liveOffset);
    };
    const follow = () => {
      top.current = element.scrollTop;
      setAway(Math.abs(element.scrollTop - liveTop()) > LIVE_EDGE_PX);
    };
    element.addEventListener("scroll", follow, { passive: true });
    // The scroller may already be away from the live edge when this mounts,
    // which is what happens when the page is switched while scrolled.
    follow();
    return () => element.removeEventListener("scroll", follow);
  }, [scroller, live, liveOffset]);

  return (
    <div className="scroll-top-anchor">
      {away && (
        <button
          type="button"
          className="scroll-top"
          onClick={() => {
            const element = scroller.current;
            if (!element) return;
            const target = live?.current;
            element.scrollTo({
              top: target ? Math.max(0, contentTop(element, target) - liveOffset) : 0,
            });
          }}
        >
          {/* The slot list is only ever scrolled away downwards, so its way
              back is up. The schedule has a list either side of its live edge
              and can be on either side of it. */}
          {live ? "Live" : "Top"} <span aria-hidden="true">{live ? "↕" : "↑"}</span>
        </button>
      )}
    </div>
  );
}

/**
 * Keeps what is on screen still while the list changes around it.
 *
 * These lists are newest first, so every new slot is inserted above what is
 * being read and pushes it down the screen — two and a half times a second,
 * which makes a scrolled list unusable.
 *
 * Browsers have scroll anchoring for exactly this and it cannot be relied on:
 * it holds the slot list but not the schedule, for reasons apparent from
 * neither, and Safari does not implement it at all.
 *
 * With an anchor, what is held is that element's distance from the top of the
 * scroller, so anything that moves it is followed rather than only growth.
 * Without one the measure is the list's height, and a viewer sitting at the top
 * is left alone: being there is a request to see what arrives.
 */
function useHeldScroll(
  scroller: RefObject<HTMLElement | null>,
  // A plain box rather than `RefObject`, whose `current` React types as
  // read-only; this one is written on both sides.
  top: { current: number },
  anchor?: RefObject<HTMLElement | null>,
): void {
  // Undefined until the first measurement rather than zero. Seeded at zero, the
  // first render reads the anchor as having moved the whole way down the list
  // and scrolls there, which is a page-length jump on arrival and again on
  // every remount.
  const measured = useRef<number | undefined>(undefined);

  useLayoutEffect(() => {
    const element = scroller.current;
    if (!element) return;

    const held = anchor?.current;
    const measure = held ? contentTop(element, held) : element.scrollHeight;
    const previous = measured.current;
    measured.current = measure;

    // Nothing to compare against yet, and nothing to hold: a list that does not
    // scroll cannot have been pushed.
    if (previous === undefined || element.scrollHeight <= element.clientHeight) {
      top.current = element.scrollTop;
      return;
    }

    // Compared before `top` is refreshed, or the check for a position something
    // else has already moved could never fire.
    const next = heldScrollTop(element.scrollTop, top.current, previous, measure, held != null);
    if (next !== element.scrollTop) {
      element.scrollTop = next;
    }
    top.current = element.scrollTop;
  });
}
