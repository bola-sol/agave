import { describe, expect, it } from "vitest";
import { SERIES, type Series } from "./ask";
import { thresholdOf } from "./askFigures";
import {
  asksEarned,
  asksProduction,
  earningsLine,
  epochAsked,
  inWindow,
  ownFigure,
  ownLabel,
  ownScope,
  ownThresholdOf,
  ownValue,
  production,
  readOwnBlocks,
  type OwnContext,
} from "./askOwn";
import { epochTotals, type BlockHead } from "./produced";
import type { FigureRow, FiguresPage } from "./types";

function series(key: string): Series {
  const found = SERIES.find((entry) => entry.key === key);
  if (!found) throw new Error(`no series ${key}`);
  return found;
}

const T = 1_764_000_000_000;

function block(slot: number, fees: number, priority: number, overrides: Partial<BlockHead> = {}): BlockHead {
  return {
    slot,
    slot_time_millis: T + slot * 400,
    transactions: 1000 + slot,
    block_cost: 40_000_000,
    block_cost_limit: 60_000_000,
    total_fees: fees,
    priority_fees: priority,
    tips: null,
    duration_nanos: 400_000_000,
    ...overrides,
  };
}

const context = (overrides: Partial<OwnContext> = {}): OwnContext => ({
  window: "epoch 1093 so far",
  threshold: null,
  clock: (millis) => `t${millis}`,
  short: null,
  ...overrides,
});

describe("ownScope", () => {
  it("reads a request about our own blocks or what we earned", () => {
    for (const typed of [
      "fees from our blocks this epoch",
      "how much did we earn in fees",
      "total revenue today",
      "blocks we produced in the last 6 hours",
      "how many slots did we skip",
      "compute in my blocks",
      "our priority fees",
      "what are searchers paying us",
      "transactions per block when we lead",
    ]) {
      expect(ownScope(typed), typed).toBe(true);
    }
  });

  it("leaves a request about the node or the cluster alone, bare we and us included", () => {
    for (const typed of [
      "Are we getting many shreds through repair? Last hour please",
      "how much are we uploading",
      "are we dropping packets",
      "Total fees over the past 6 hours",
      "fees by leader client",
      "replay over 900 us",
      "Shreds per second over the past hour",
    ]) {
      expect(ownScope(typed), typed).toBe(false);
    }
  });

  it("reads an epoch named by word", () => {
    expect(epochAsked("what did we earn last epoch")).toBe("last");
    expect(epochAsked("fees from our blocks in the previous epoch")).toBe("last");
    expect(epochAsked("our fees this epoch")).toBe("this");
    expect(epochAsked("our fees in the last hour")).toBeNull();
  });

  it("tells earning and production requests apart", () => {
    expect(asksEarned("how much did we earn")).toBe(true);
    expect(asksEarned("fees from our blocks")).toBe(false);
    expect(asksProduction("how many of our leader slots did we produce")).toBe(true);
    expect(asksProduction("blocks we skipped this epoch")).toBe(true);
    expect(asksProduction("skip rate last 12 hours")).toBe(false);
  });
});

describe("ownValue", () => {
  it("gives the fees a block earned by the Slots page's arithmetic, and the rest of its figures", () => {
    // 10,000 lamports in all, 4,000 of them priority: half the base is burned.
    const held = block(10, 10_000, 4_000);
    expect(ownValue("fees", held, undefined)).toBeCloseTo((3_000 + 4_000) / 1e9);
    expect(ownValue("priority_fees", held, undefined)).toBeCloseTo(4_000 / 1e9);
    expect(ownValue("compute", held, undefined)).toBe(40);
    expect(ownValue("transactions", held, undefined)).toBe(1010);
    expect(ownValue("tips", held, undefined)).toBeNull();
    expect(ownValue("replay", held, undefined)).toBeNull();
  });
});

describe("ownFigure", () => {
  const held = [block(10, 10_000, 4_000), block(14, 30_000, 10_000), block(18, 2_000, 0)];

  it("totals what our blocks earned, the same as the Slots page's epoch row", () => {
    const answer = ownFigure("total", series("fees"), held, undefined, context());
    const row = epochTotals(held, () => 1093).get(1093);
    expect(answer).toMatchObject({ label: "Fees earned from our blocks, epoch 1093 so far", warn: false });
    // 3,000 + 10,000 + 1,000 base kept, 14,000 priority.
    expect(row!.earned).toBe(28_000);
    expect(answer).toMatchObject({
      value: "0.000028 SOL",
      detail: "3 of our blocks · base fees after the burn 0.000014, priority 0.000014",
    });
  });

  it("gives the highest and lowest block with its slot, and a mean per block", () => {
    expect(ownFigure("highest", series("transactions"), held, undefined, context())).toMatchObject({
      value: "1,018 per block",
      detail: `slot 18 at t${T + 18 * 400}`,
    });
    expect(ownFigure("mean", series("compute"), held, undefined, context())).toMatchObject({ value: "40 million CU per block" });
  });

  it("counts our blocks over a value", () => {
    const over = thresholdOf("priority fees over 0.000005 SOL", series("priority_fees"));
    expect(ownFigure("when_above", series("priority_fees"), held, undefined, context({ threshold: over }))).toMatchObject({
      label: "1 of our 3 blocks over 0.000005 SOL per block",
    });
  });

  it("says when there are no blocks of ours, and only then whether tips are read", () => {
    expect(ownFigure("total", series("fees"), [], undefined, context())).toEqual({ unfit: "No blocks of ours, epoch 1093 so far." });
    expect(ownFigure("total", series("tips"), [], undefined, context())).toEqual({ unfit: "No blocks of ours, epoch 1093 so far." });
    expect(ownFigure("total", series("tips"), held, undefined, context())).toEqual({ unfit: "Tips are not read on this build." });
    // A build that reads tips gives a figure.
    const tipped = held.map((one) => ({ ...one, tips: 1_000_000 }));
    expect(ownFigure("total", series("tips"), tipped, { jito_cut_bps: 300, commission_bps: 800 }, context())).toHaveProperty("value");
  });

  it("reads a transactions value per block, as our blocks give it", () => {
    const over = ownThresholdOf("our blocks with transactions over 1,015 per block", series("transactions"));
    expect(over).toMatchObject({ value: 1015, above: true });
    expect(ownFigure("when_above", series("transactions"), held, undefined, context({ threshold: over }))).toMatchObject({
      label: "1 of our 3 blocks over 1,015 per block",
    });
    expect(ownThresholdOf("transactions over 1500 tps in our blocks", series("transactions"))).toHaveProperty("wrong");
  });

  it("carries a shortfall of blocks held", () => {
    const answer = ownFigure("mean", series("fees"), held, undefined, context({ short: "3 of 5 blocks held" })) as { detail: string; warn: boolean };
    expect(answer.detail).toBe("3 of our blocks · 3 of 5 blocks held");
    expect(answer.warn).toBe(true);
  });
});

describe("ownLabel", () => {
  it("names a series as it is for our blocks", () => {
    expect(ownLabel(series("fees"))).toEqual({ head: "Fees earned from our blocks", unit: "SOL" });
    expect(ownLabel(series("replay")).head).toBe("Replay time");
  });
});

describe("production", () => {
  it("gives what was produced of the slots passed, as the header counts them", () => {
    expect(production({ epoch: 1093, slots: 312, passed: 200, produced: 196, skipped: 4 }, { window: "epoch 1093 so far", short: null })).toEqual({
      label: "Our leader slots, epoch 1093 so far",
      value: "196 of 200 produced",
      detail: "4 skipped · 2% skip rate · 112 to come",
      warn: false,
    });
    expect(production(undefined, { window: "epoch 1093", short: null })).toEqual({ unfit: "The leader slot counts for that epoch are not held." });
    // After a restart with a short ledger: only 100 of the 400 passed are answered.
    expect(production({ epoch: 1093, slots: 400, passed: 400, produced: 95, skipped: 5 }, { window: "epoch 1093", short: null })).toMatchObject({
      value: "95 of 100 produced",
      detail: "5 skipped · 5% skip rate · 300 too old for the slot history",
    });
  });
});

describe("earningsLine", () => {
  it("gives the card's own line, only for the epoch asked about", () => {
    const earnings = { epoch: 1093, voting: 30e9, voting_lost: 0, voting_max: 31e9, bonus: 9.69e9, bonus_lost: 0, bonus_max: 10e9, commission_bps: 400, unread: 0 };
    const line = earningsLine(earnings, 1093) as { label: string; value: string };
    expect(line.label).toBe("Voting and producer bonus, epoch 1,093 so far");
    expect(line.value).toMatch(/^1\.58\d SOL, 4% of 39\.69$/);
    expect(earningsLine(earnings, 1092)).toBeNull();
    expect(earningsLine(null, 1093)).toBeNull();
  });
});

describe("readOwnBlocks", () => {
  const row = (head: BlockHead): FigureRow =>
    [head.slot, head.slot_time_millis, head.transactions, head.block_cost, head.block_cost_limit, head.total_fees, head.priority_fees, head.tips, head.duration_nanos] as FigureRow;
  /** Pages of two blocks, newest first. */
  function pages(slots: number[]) {
    const asked: (number | undefined)[] = [];
    const page = (before: number | undefined): Promise<FiguresPage | null> => {
      asked.push(before);
      const older = slots.filter((slot) => before === undefined || slot < before).sort((a, b) => b - a);
      const figures = older.slice(0, 2).map((slot) => row(block(slot, 1, 0)));
      return Promise.resolve({ figures, turns: [], held: slots.length, floor: 0, next: older.length > 2 ? older[1] : null });
    };
    return { asked, page };
  }

  it("reads pages newest first until one reaches back past the epoch", async () => {
    const { asked, page } = pages([2, 4, 6, 8, 10, 12]);
    const read = await readOwnBlocks(page, { kind: "epoch", epoch: 1, first: 7, last: 100 });
    expect(read.blocks.map((one) => one.slot)).toEqual([8, 10, 12]);
    expect(asked).toEqual([undefined, 10]);
    expect(read.reached).toBe(true);
  });

  it("says when the list runs out before the window, and stops at a time window's start", async () => {
    const short = await readOwnBlocks(pages([8, 10, 12]).page, { kind: "epoch", epoch: 1, first: 2, last: 100 });
    expect(short).toMatchObject({ reached: false });
    expect(short.blocks.map((one) => one.slot)).toEqual([8, 10, 12]);
    const { asked, page } = pages([2, 4, 6, 8, 10, 12]);
    const timed = await readOwnBlocks(page, { kind: "time", from: T + 9 * 400, to: T + 100 * 400 });
    expect(timed.blocks.map((one) => one.slot)).toEqual([10, 12]);
    expect(timed.reached).toBe(true);
    expect(asked).toEqual([undefined, 10]);
    const capped = await readOwnBlocks(pages([2, 4, 6, 8, 10, 12]).page, { kind: "epoch", epoch: 1, first: 0, last: 100 }, 2);
    expect(capped).toMatchObject({ reached: false });
  });

  it("fails on a refused page rather than giving a short list", async () => {
    let calls = 0;
    const refused = (): Promise<FiguresPage | null> => {
      calls += 1;
      return calls === 1 ? pages([2, 4, 6, 8, 10, 12]).page(undefined) : Promise.resolve(null);
    };
    await expect(readOwnBlocks(refused, { kind: "epoch", epoch: 1, first: 0, last: 100 })).rejects.toThrow();
  });

  it("takes a time window by each block's time", () => {
    expect(inWindow(block(10, 1, 0), { kind: "time", from: T, to: T + 4001 })).toBe(true);
    expect(inWindow(block(10, 1, 0), { kind: "time", from: T + 4001, to: T + 9000 })).toBe(false);
    expect(inWindow(block(10, 1, 0, { slot_time_millis: null }), { kind: "time", from: 0, to: Infinity })).toBe(false);
  });
});
