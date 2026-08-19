import { describe, expect, it } from "vitest";
import { groupByLeader, matchesQuery, type Led } from "./schedule";

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
