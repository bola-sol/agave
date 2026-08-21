import { describe, expect, it } from "vitest";
import type { Waterfall } from "./types";
import { scheduledShare, waterfallRows } from "./waterfall";

/** A window in which nothing happened, to be overridden a field at a time. */
function quiet(over: Partial<Waterfall> = {}): Waterfall {
  return {
    received: 0,
    not_held: 0,
    check_queue_full: 0,
    unparsable: 0,
    bad_locks: 0,
    compute_budget: 0,
    too_old: 0,
    already_processed: 0,
    fee_payer: 0,
    filtered: 0,
    nonce_conflict: 0,
    buffered: 0,
    queue_full: 0,
    nonce_evicted: 0,
    cleared: 0,
    cleaned: 0,
    scheduled: 0,
    blocked_conflicts: 0,
    blocked_threads: 0,
    finished: 0,
    retried: 0,
    ...over,
  };
}

/** A validator mid-leader-slot, with the receive stretch balancing exactly. */
function busy(): Waterfall {
  return quiet({
    received: 1000,
    not_held: 800,
    check_queue_full: 10,
    unparsable: 20,
    bad_locks: 5,
    compute_budget: 5,
    too_old: 30,
    already_processed: 40,
    fee_payer: 6,
    filtered: 0,
    nonce_conflict: 4,
    buffered: 80,
    queue_full: 12,
    nonce_evicted: 1,
    cleared: 7,
    cleaned: 2,
    scheduled: 60,
    blocked_conflicts: 25,
    blocked_threads: 3,
    finished: 58,
    retried: 2,
  });
}

describe("waterfallRows", () => {
  it("accounts for every received transaction across the first stretch", () => {
    // The identity the validator's own tests assert, restated here because it
    // is what makes the top section an account rather than a list of numbers
    // that happen to sit under a heading. If a counter is ever added upstream
    // and not mapped, this is what notices.
    const w = busy();
    const rows = waterfallRows(w);
    const upToBuffered = rows.slice(
      rows.findIndex((row) => row.key === "not_held"),
      rows.findIndex((row) => row.key === "buffered") + 1,
    );
    const accounted = upToBuffered.reduce((sum, row) => sum + row.count, 0);
    expect(accounted).toBe(w.received);
  });

  it("measures every bar against what arrived, not against the stage above", () => {
    // So the lengths are comparable the whole way down. Renormalising each
    // section against its own heading would draw a loss of eighty out of eighty
    // as long as the received bar itself.
    const rows = waterfallRows(busy());
    const buffered = rows.find((row) => row.key === "buffered");
    expect(buffered?.share).toBeCloseTo(0.08, 10);
    const notHeld = rows.find((row) => row.key === "not_held");
    expect(notHeld?.share).toBeCloseTo(0.8, 10);
  });

  it("draws the same rows in the same order whatever happened", () => {
    // The card must not change height under someone reading it, and a zero is
    // itself worth reading — no fee payer failed is not the same as nothing
    // counts fee payer failures.
    const busyKeys = waterfallRows(busy()).map((row) => row.key);
    const quietKeys = waterfallRows(quiet()).map((row) => row.key);
    expect(quietKeys).toEqual(busyKeys);
    expect(busyKeys.length).toBe(21);
  });

  it("divides nothing by nothing when the window is empty", () => {
    for (const row of waterfallRows(quiet())) {
      expect(Number.isFinite(row.share)).toBe(true);
      expect(row.share).toBe(0);
    }
  });

  it("marks the three stages apart from their reasons", () => {
    const rows = waterfallRows(busy());
    const stages = rows.filter((row) => row.kind === "stage").map((row) => row.key);
    expect(stages).toEqual(["received", "buffered", "scheduled", "finished"]);
  });

  it("counts held-back work as a note rather than a loss", () => {
    // Nothing is lost when the scheduler cannot place a transaction this pass;
    // it waits. Counting it as loss would make a contended slot look like a
    // failing one.
    const rows = waterfallRows(busy());
    const blocked = rows.filter((row) => row.key.startsWith("blocked_"));
    expect(blocked.map((row) => row.kind)).toEqual(["note", "note"]);
    expect(rows.find((row) => row.key === "retried")?.kind).toBe("note");
  });
});

describe("scheduledShare", () => {
  it("measures against what the validator kept, not what reached it", () => {
    // Against received it would read 6%, which is a statement about how much of
    // the cluster's traffic this node was due to execute rather than about the
    // node. Against what it held it is 75%, which is about the node.
    expect(scheduledShare(busy())).toBeCloseTo(60 / 80, 10);
  });

  it("says nothing when the validator has held nothing", () => {
    // The ordinary state of a node that has not been leader recently, not a
    // failure to schedule.
    expect(scheduledShare(quiet())).toBeNull();
    expect(scheduledShare(quiet({ received: 5000, not_held: 5000 }))).toBeNull();
  });

  it("does not report more scheduled than held", () => {
    // The two counts are different populations a window apart, so a queue
    // draining faster than it fills genuinely reports it. Over 100% reads as a
    // bug in the page rather than as a queue draining.
    expect(scheduledShare(quiet({ buffered: 10, scheduled: 40 }))).toBe(1);
  });
});
