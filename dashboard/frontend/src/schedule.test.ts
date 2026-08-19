import { describe, expect, it } from "vitest";
import {
  groupByLeader,
  hasBegun,
  matchesQuery,
  prependedCount,
  rowKey,
  scheduleRows,
  timeline,
  type Led,
} from "./schedule";
import type { SlotEntry, UpcomingSlot } from "./types";

function slot(number: number, leader: string, name: string | null = null): Led {
  return {
    slot: number,
    leader,
    leader_name: name,
    leader_icon: null,
    mine: false,
  };
}

describe("groupByLeader", () => {
  it("gathers a leader's run into one group", () => {
    const groups = groupByLeader([
      slot(103, "alice"),
      slot(102, "alice"),
      slot(101, "alice"),
      slot(100, "alice"),
    ]);
    expect(groups).toHaveLength(1);
    expect(groups[0].slots.map((entry) => entry.slot)).toEqual([103, 102, 101, 100]);
  });

  it("starts a new group when the leader changes", () => {
    const groups = groupByLeader([slot(101, "alice"), slot(100, "bob")]);
    expect(groups.map((group) => group.leader)).toEqual(["alice", "bob"]);
  });

  it("starts a new group across a gap even when the leader is the same", () => {
    // Two separate turns at leading. Drawn as one they would claim a run that
    // never happened.
    const groups = groupByLeader([slot(200, "alice"), slot(100, "alice")]);
    expect(groups).toHaveLength(2);
  });

  it("has nothing to say about an empty list", () => {
    expect(groupByLeader([])).toEqual([]);
  });
});

describe("matchesQuery", () => {
  const group = groupByLeader([slot(430789128, "J7v9KQ8s", "Staking Facilities")])[0];

  it("matches a name whatever its case", () => {
    expect(matchesQuery(group, "staking")).toBe(true);
    expect(matchesQuery(group, "STAKING")).toBe(true);
  });

  it("matches part of the leader key", () => {
    expect(matchesQuery(group, "J7v9")).toBe(true);
  });

  it("matches a slot in the group", () => {
    expect(matchesQuery(group, "430789128")).toBe(true);
    expect(matchesQuery(group, "789")).toBe(true);
  });

  it("matches everything when nothing was asked", () => {
    expect(matchesQuery(group, "")).toBe(true);
    expect(matchesQuery(group, "   ")).toBe(true);
  });

  it("does not match something absent", () => {
    expect(matchesQuery(group, "nansen")).toBe(false);
  });
});

function held(slot: number, leader: string): SlotEntry {
  return {
    slot,
    level: "completed",
    leader,
    leader_name: null,
    leader_icon: null,
    mine: false,
    block: null,
    duration_nanos: null,
  };
}

function scheduled(slot: number, leader: string): UpcomingSlot {
  return { slot, leader, leader_name: null, leader_icon: null, mine: false };
}

describe("timeline", () => {
  it("reads the two sources as one run, newest first", () => {
    const rows = timeline([held(100, "alice"), held(101, "alice")], [
      scheduled(102, "alice"),
      scheduled(103, "alice"),
    ]);
    expect(rows.map((row) => row.slot)).toEqual([103, 102, 101, 100]);
    expect(rows.map((row) => row.entry !== null)).toEqual([false, false, true, true]);
  });

  it("prefers the slot that happened over the one that was promised", () => {
    // The store knows what happened; the schedule only knows who was asked to.
    const rows = timeline([held(100, "alice")], [scheduled(100, "alice")]);
    expect(rows).toHaveLength(1);
    expect(rows[0].entry).not.toBeNull();
  });

  it("gathers a straddling turn into one group rather than two", () => {
    // The whole point: without the merge this leader appears twice, shrinking
    // above the boundary and growing below it as its slots are produced.
    const rows = timeline(
      [held(100, "alice"), held(101, "alice")],
      [scheduled(102, "alice"), scheduled(103, "alice")],
    );
    const groups = groupByLeader(rows);
    expect(groups).toHaveLength(1);
    expect(groups[0].slots).toHaveLength(4);
  });
});

describe("hasBegun", () => {
  it("is true once the turn's own first slot has started", () => {
    const groups = groupByLeader(
      timeline([held(100, "alice")], [scheduled(101, "alice"), scheduled(102, "alice")]),
    );
    expect(hasBegun(groups[0])).toBe(true);
  });

  it("is false while every slot is still only scheduled", () => {
    const groups = groupByLeader(
      timeline([], [scheduled(101, "alice"), scheduled(102, "alice")]),
    );
    expect(hasBegun(groups[0])).toBe(false);
  });
});

describe("scheduleRows", () => {
  const rows = (heldSlots: number[], scheduledSlots: number[]) =>
    scheduleRows(
      groupByLeader(
        timeline(
          heldSlots.map((slot) => held(slot, `L${Math.floor(slot / 4)}`)),
          scheduledSlots.map((slot) => scheduled(slot, `L${Math.floor(slot / 4)}`)),
        ),
      ),
    );

  it("puts the turns to come above the boundary and the rest below", () => {
    const list = rows([100, 101, 102, 103], [104, 105, 106, 107]);
    expect(list.map((row) => (row.kind === "heading" ? row.label : "turn"))).toEqual([
      "Upcoming",
      "turn",
      "Produced",
      "turn",
    ]);
  });

  it("keeps the boundary even with nothing scheduled yet", () => {
    // The heading is what the page measures itself against, so it cannot come
    // and go with the data.
    const list = rows([100, 101], []);
    expect(list[0]).toEqual({ kind: "heading", label: "Produced" });
  });
});

describe("rowKey", () => {
  it("names a turn by its own first slot, not its position", () => {
    // Turns arrive above and fall off below constantly; a name that moved with
    // them would identify nothing.
    const [group] = groupByLeader(timeline([held(100, "a"), held(101, "a")], []));
    expect(rowKey({ kind: "group", group })).toBe("turn:100");
  });
});

describe("prependedCount", () => {
  it("counts what appeared above the row that used to be first", () => {
    expect(prependedCount(["b", "c"], ["a", "b", "c"])).toBe(1);
    expect(prependedCount(["c"], ["a", "b", "c"])).toBe(2);
  });

  it("counts nothing when the front has not moved", () => {
    expect(prependedCount(["a", "b"], ["a", "b", "c"])).toBe(0);
  });

  it("counts nothing across a list with nothing in common", () => {
    // A search was typed, or the connection dropped: there is no position to
    // hold, and guessing one would throw the list somewhere arbitrary.
    expect(prependedCount(["a"], ["x", "y"])).toBe(0);
    expect(prependedCount([], ["a", "b"])).toBe(0);
  });
});
