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
 * A pill that returns a scrolled list to its live edge.
 *
 * `live` names an element that should sit at the top when the list is where it
 * belongs; without it the live edge is the top of the scroller. The schedule
 * needs the first: its live edge is the boundary between what has been produced
 * and what is still to come, which has the schedule above it and history below,
 * so both directions are somewhere to go.
 *
 * Both the slot list and the schedule are newest-first and hundreds of rows
 * long, so the top of the list is where the live data is and scrolling back by
 * hand is a long way. Takes the scroller rather than finding one, because the
 * two live in different parts of the tree.
 *
 * The wrapper carries no height, so the button hangs over the rows instead of
 * moving them, and is sticky rather than absolute so it tracks the list it
 * belongs to and not the page.
 *
 * Also holds the scroll position while the list grows at the top, which is the
 * other half of making a live list readable; see [`useHeldScroll`].
 */
export function ScrollTop({
  scroller,
  live,
}: {
  scroller: RefObject<HTMLElement | null>;
  live?: RefObject<HTMLElement | null>;
}) {
  const [away, setAway] = useState(false);
  // Shared with the hook below: it needs to know where the list was left, to
  // tell its own correction apart from one the browser already made.
  const top = useRef(0);
  useHeldScroll(scroller, top, live !== undefined);

  useEffect(() => {
    const element = scroller.current;
    if (!element) return;

    const liveTop = () => {
      const target = live?.current;
      return target ? target.offsetTop - element.offsetTop : 0;
    };
    const follow = () => {
      top.current = element.scrollTop;
      setAway(Math.abs(element.scrollTop - liveTop()) > LIVE_EDGE_PX);
    };
    element.addEventListener("scroll", follow, { passive: true });
    // The scroller may already be away from the top when this mounts, which is
    // what happens when the page is switched while scrolled down.
    follow();
    return () => element.removeEventListener("scroll", follow);
  }, [scroller, live]);

  return (
    <div className="scroll-top-anchor">
      {away && (
        <button
          type="button"
          className="scroll-top"
          onClick={() => {
            const element = scroller.current;
            const target = live?.current;
            element?.scrollTo({
              top: target ? target.offsetTop - element.offsetTop : 0,
            });
          }}
        >
          Top <span aria-hidden="true">↑</span>
        </button>
      )}
    </div>
  );
}

/**
 * Keeps what is on screen still while rows arrive above it.
 *
 * These lists are newest first, so every new slot is inserted at the top and
 * pushes everything being read down the screen — two and a half times a second,
 * which makes a scrolled list unusable.
 *
 * Browsers have scroll anchoring for exactly this, and it cannot be relied on:
 * it holds the slot list but not the schedule, for reasons that are not
 * apparent from either, and Safari does not implement it at all. Measuring the
 * growth and adding it is what anchoring would have done, in every engine.
 *
 * Only while scrolled away: a viewer sitting at the live edge wants the new
 * rows, which is the whole point of being there.
 */
function useHeldScroll(
  scroller: RefObject<HTMLElement | null>,
  // A plain box rather than `RefObject`, whose `current` React types as
  // read-only; this one is written on both sides.
  top: { current: number },
  holdAtTop: boolean,
): void {
  const height = useRef(0);

  useLayoutEffect(() => {
    const element = scroller.current;
    if (!element) return;

    const held = heldScrollTop(
      element.scrollTop,
      top.current,
      height.current,
      element.scrollHeight,
      holdAtTop,
    );
    height.current = element.scrollHeight;
    if (held !== element.scrollTop) {
      element.scrollTop = held;
    }
    top.current = element.scrollTop;
  });
}
