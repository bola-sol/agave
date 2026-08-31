import { describe, expect, it } from "vitest";
import { entriesOf, HAS_BLOCK, HAS_CLOCK, leaderAt, leaderDisplays, type SlotRange, type WireRow } from "./slotHistory";
import type { EpochInfo, SlotEntry } from "./types";

const ALICE = "A1ice1111111111111111111111111111111111111";
const BOB = "B0b22222222222222222222222222222222222222222";

function epochOf(over: Partial<EpochInfo> = {}): EpochInfo {
  return {
    epoch: 842,
    start_slot: 1000,
    end_slot: 1015,
    slots_in_epoch: 16,
    my_leader_slots: [],
    leaders: [ALICE, BOB],
    // Four turns of four slots: Alice, Bob, Alice, Bob.
    turns: [0, 1, 0, 1],
    block_cost_limit: 60_000_000,
    account_cost_limit: 12_000_000,
    ...over,
  };
}

function row(over: Partial<Record<number, number>> = {}): WireRow {
  const base: WireRow = [3, HAS_BLOCK | HAS_CLOCK, 66, 8_752, 11_877_602, 44_100_000, 1_000_000];
  return base.map((value, index) => over[index] ?? value) as WireRow;
}

describe("leaderAt", () => {
  it("finds the leader through one index rather than a search", () => {
    expect(leaderAt(epochOf(), 1000)).toBe(ALICE);
    expect(leaderAt(epochOf(), 1003)).toBe(ALICE);
    expect(leaderAt(epochOf(), 1004)).toBe(BOB);
    expect(leaderAt(epochOf(), 1015)).toBe(BOB);
  });

  it("names nobody outside the epoch the arrays describe", () => {
    // Two epochs are in play either side of a boundary, and answering from the
    // wrong one would name a leader confidently and wrongly.
    expect(leaderAt(epochOf(), 999)).toBeNull();
    expect(leaderAt(epochOf(), 1016)).toBeNull();
  });

  it("names nobody where the validator could not derive the schedule", () => {
    // Sent as an empty array rather than a partial one, so this is the whole
    // of the check.
    expect(leaderAt(epochOf({ turns: [] }), 1000)).toBeNull();
    expect(leaderAt(undefined, 1000)).toBeNull();
  });
});

describe("entriesOf", () => {
  const range = (rows: (WireRow | null)[]): SlotRange => ({ first_slot: 1000, rows });

  it("reads the columns in the order the validator writes them", () => {
    // The one place the wire order is pinned on this side. It is positional, so
    // a silent reordering would put fees in the compute column and nothing
    // would fail until someone read the page.
    const [entry] = entriesOf(range([row()]), epochOf(), new Map(), undefined);
    expect(entry.level).toBe("rooted");
    expect(entry.block?.non_vote_transactions).toBe(8_752);
    expect(entry.block?.transactions).toBe(66 + 8_752);
    expect(entry.block?.block_cost).toBe(11_877_602);
    expect(entry.block?.total_fees).toBe(44_100_000);
  });

  it("takes the cost limits from the epoch rather than from the row", () => {
    // They are the same two numbers for the epoch's whole life, which is why
    // they are not on the row at all.
    const [entry] = entriesOf(range([row()]), epochOf(), new Map(), undefined);
    expect(entry.block?.block_cost_limit).toBe(60_000_000);
  });

  it("works the duration out as the gap to the last slot that had a clock", () => {
    // Not carried, because it is a subtraction of two things that are.
    const entries = entriesOf(
      range([row({ 6: 1_000_000 }), row({ 6: 1_000_400 })]),
      epochOf(),
      new Map(),
      undefined,
    );
    expect(entries[0].duration_nanos).toBeNull();
    expect(entries[1].duration_nanos).toBe(400_000_000);
  });

  it("carries the gap across a slot it has no row for", () => {
    // A skipped slot shows as one long interval rather than as none, which is
    // what the validator's own walk does with it.
    const entries = entriesOf(
      range([row({ 6: 1_000_000 }), null, row({ 6: 1_000_800 })]),
      epochOf(),
      new Map(),
      undefined,
    );
    expect(entries).toHaveLength(2);
    expect(entries[1].slot).toBe(1002);
    expect(entries[1].duration_nanos).toBe(800_000_000);
  });

  it("leaves a block out where none was recorded, rather than drawing an empty one", () => {
    const [entry] = entriesOf(range([row({ 1: HAS_CLOCK })]), epochOf(), new Map(), undefined);
    expect(entry.block).toBeNull();
  });

  it("marks the slots we led", () => {
    const entries = entriesOf(range([row(), null, null, null, row()]), epochOf(), new Map(), ALICE);
    expect(entries[0].mine).toBe(true);
    expect(entries[1].mine).toBe(false);
  });

  it("names a leader from what the live window taught the page", () => {
    const displays = leaderDisplays([
      { leader: ALICE, leader_name: "Alice Co", leader_icon: "https://a/i.png" } as SlotEntry,
    ]);
    const [entry] = entriesOf(range([row()]), epochOf(), displays, undefined);
    expect(entry.leader).toBe(ALICE);
    expect(entry.leader_name).toBe("Alice Co");
  });

  it("falls back to the key for a leader the live window never showed", () => {
    // Names are the largest strings a slot used to repeat, so the epoch arrays
    // carry keys only. A turn from before anything was watched gets its key.
    const [entry] = entriesOf(range([row()]), epochOf(), new Map(), undefined);
    expect(entry.leader).toBe(ALICE);
    expect(entry.leader_name).toBeNull();
  });
});

describe("leaderDisplays", () => {
  it("keeps the first naming of a leader and ignores the ones with nothing to add", () => {
    const displays = leaderDisplays([
      { leader: ALICE, leader_name: null, leader_icon: null } as SlotEntry,
      { leader: ALICE, leader_name: "Alice Co", leader_icon: null } as SlotEntry,
    ]);
    expect(displays.get(ALICE)?.name).toBe("Alice Co");
  });
});
