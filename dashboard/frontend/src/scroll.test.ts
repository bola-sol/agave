import { describe, expect, it } from "vitest";
import { heldScrollTop } from "./scroll";

describe("heldScrollTop", () => {
  it("moves down by whatever arrived above, so the view does not", () => {
    expect(heldScrollTop(900, 900, 5000, 5116)).toBe(1016);
  });

  it("leaves a viewer at the live edge alone", () => {
    // Being at the top is a request to see what arrives next.
    expect(heldScrollTop(0, 0, 5000, 5116)).toBe(0);
  });

  it("does nothing when the list did not grow", () => {
    expect(heldScrollTop(900, 900, 5000, 5000)).toBe(900);
  });

  it("does not drag the view when the list shrinks", () => {
    // Slots are pruned from the bottom, which moves nothing above them.
    expect(heldScrollTop(900, 900, 5000, 4800)).toBe(900);
  });

  it("holds the top too where the top is not the live edge", () => {
    // The schedule settles on the boundary between produced and scheduled, so
    // there is more schedule above it and nothing at the top means "follow".
    expect(heldScrollTop(0, 0, 5000, 5116, true)).toBe(116);
  });

  it("follows an anchor upwards as well as down", () => {
    // Anchored, the measure moves both ways: turns cross the boundary from
    // above it and the schedule is trimmed behind them. A height only grows.
    expect(heldScrollTop(900, 900, 5000, 4880, true)).toBe(780);
    expect(heldScrollTop(900, 900, 5000, 4880, false)).toBe(900);
  });

  it("never asks for a position above the top", () => {
    expect(heldScrollTop(40, 40, 5000, 4800, true)).toBe(0);
  });

  it("stands aside when the browser already anchored", () => {
    // Where scroll anchoring fires it has already added the growth. Adding it
    // again would send the list twice as far as it should go.
    expect(heldScrollTop(1016, 900, 5000, 5116)).toBe(1016);
  });
});
