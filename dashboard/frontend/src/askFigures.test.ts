import { describe, expect, it } from "vitest";
import { bucket, SERIES, windowOf, type Bins, type Series, type SlotSeries } from "./ask";
import {
  answerFigure,
  comparing,
  directed,
  lasting,
  NO_SHARE,
  OPERATION_PROMPT,
  operationSchema,
  OPERATIONS,
  parseOperation,
  readAs,
  shareFigure,
  sharePair,
  spansOf,
  thresholdOf,
  unfit,
  withoutThreshold,
  type FigureContext,
  type Threshold,
} from "./askFigures";
import { HAS_BLOCK, HAS_CLOCK, HAS_REPLAY, HAS_SHREDS, HAS_TIPS, type WireRow } from "./slotHistory";

function series(key: string): Series {
  const found = SERIES.find((entry) => entry.key === key);
  if (!found) throw new Error(`no series ${key}`);
  return found;
}

const slot = (key: string): SlotSeries => series(key) as SlotSeries;

const ALL = HAS_BLOCK | HAS_CLOCK | HAS_TIPS | HAS_REPLAY | HAS_SHREDS;

/** A block at `time` with `replay` microseconds of replay and `failed` failed transactions. */
function row(time: number, replay: number, failed = 10, nonVotes = 100): WireRow {
  return [4, ALL, 2000, nonVotes, 40_000_000, 3_000_000_000, 1_000_000_000, 500_000_000, time, replay, 1300, 7, 380, 395, 12, 31, failed];
}

const threshold = (read: ReturnType<typeof thresholdOf>): Threshold => {
  if (!read || "wrong" in read) throw new Error(`no threshold: ${JSON.stringify(read)}`);
  return read;
};

const context = (overrides: Partial<FigureContext> = {}): FigureContext => ({
  window: "past hour",
  threshold: null,
  comparing: false,
  clock: (millis) => `t${millis}`,
  ...overrides,
});

describe("the operation prompt", () => {
  it("is the text measured on the built-in model", () => {
    expect(OPERATION_PROMPT).toHaveLength(527);
    expect(OPERATION_PROMPT.split("\n")).toEqual([
      "A request asks about things recorded about a Solana validator and the machine it runs on. Answer the operation it asks for.",
      "chart: draw them over time. Most requests want this, including a plain name or a question about how something has been.",
      "total: a sum over the window, how much or how many in all.",
      "mean: an average or a typical value.",
      "highest or lowest: a peak or maximum, or a minimum.",
      "share: what share, fraction or percentage one thing is of another.",
      "when_above or when_below: when something went over or under a value.",
    ]);
  });

  it("allows each operation and nothing open", () => {
    expect(operationSchema()).toEqual({
      type: "object",
      properties: { operation: { type: "string", enum: [...OPERATIONS] } },
      required: ["operation"],
      additionalProperties: false,
    });
  });

  it("reads the operation named, and nothing outside the schema", () => {
    expect(parseOperation('{"operation":"total"}')).toBe("total");
    expect(parseOperation(' {"operation": "when_above"}\n')).toBe("when_above");
    expect(parseOperation('{"operation":"sum"}')).toBeNull();
    expect(parseOperation('{"series":["fees"]}')).toBeNull();
    expect(parseOperation("total")).toBeNull();
    expect(parseOperation("")).toBeNull();
  });
});

describe("sharePair", () => {
  it("takes the second series picked as the whole, either way round", () => {
    expect(sharePair([series("failed"), series("transactions")])).toEqual({
      part: series("failed"),
      whole: series("transactions"),
      byRule: false,
    });
    expect(sharePair([series("shreds"), series("repaired")])).toEqual({
      part: series("repaired"),
      whole: series("shreds"),
      byRule: false,
    });
  });

  it("takes the whole by rule when one part was picked", () => {
    expect(sharePair([series("priority_fees")])).toEqual({ part: series("priority_fees"), whole: series("fees"), byRule: true });
    expect(sharePair([series("recovered")])?.whole).toBe(series("shreds"));
  });

  it("is null for anything else", () => {
    expect(sharePair([series("cpu")])).toBeNull();
    expect(sharePair([series("transactions")])).toBeNull();
    expect(sharePair([series("fees"), series("tips")])).toBeNull();
    expect(sharePair([])).toBeNull();
  });
});

describe("directed", () => {
  it("takes the worst, slowest or longest of a time for its highest, and the best for its lowest", () => {
    expect(directed("lowest", "worst replay time today", series("replay"))).toBe("highest");
    expect(directed("lowest", "slowest block fill in the past hour", series("fill"))).toBe("highest");
    expect(directed("lowest", "longest replay this hour", series("replay"))).toBe("highest");
    expect(directed("highest", "fastest replay this hour", series("replay"))).toBe("lowest");
    expect(directed("highest", "best time to full today", series("fill"))).toBe("lowest");
  });

  it("leaves other series, other operations and plain requests as read", () => {
    expect(directed("lowest", "worst tps today", series("transactions"))).toBe("lowest");
    expect(directed("mean", "worst replay time", series("replay"))).toBe("mean");
    expect(directed("chart", "slowest replay", series("replay"))).toBe("chart");
    expect(directed("lowest", "lowest replay time", series("replay"))).toBe("lowest");
    expect(directed(null, "worst replay time", series("replay"))).toBeNull();
  });
});

describe("unfit", () => {
  it("refuses a total of a level or a time", () => {
    expect(unfit("total", series("cpu"))).toBe("A total of CPU use is not meaningful.");
    expect(unfit("total", series("replay"))).toBe("A total of replay time is not meaningful.");
    expect(unfit("total", series("memory"))).toBe("A total of memory in use is not meaningful.");
  });

  it("allows a total of what is counted, and every other operation", () => {
    for (const key of ["shreds", "transactions", "failed", "fees", "tips", "compute", "udp_drops", "net_in", "net_out"]) {
      expect(unfit("total", series(key))).toBeNull();
    }
    expect(unfit("mean", series("cpu"))).toBeNull();
    expect(unfit("highest", series("replay"))).toBeNull();
  });
});

describe("thresholdOf", () => {
  it("reads a value in the series' own unit, or bare", () => {
    expect(threshold(thresholdOf("when was replay over 20 ms", series("replay")))).toMatchObject({ above: true, value: 20 });
    expect(threshold(thresholdOf("when was replay over 20ms", series("replay"))).value).toBe(20);
    expect(threshold(thresholdOf("replay above 25", series("replay"))).value).toBe(25);
    expect(threshold(thresholdOf("when did CPU go above 80 percent", series("cpu"))).value).toBe(80);
    expect(threshold(thresholdOf("cpu over 80%", series("cpu"))).value).toBe(80);
    expect(threshold(thresholdOf("times when tps fell below 50", series("transactions")))).toMatchObject({ above: false, value: 50 });
    expect(threshold(thresholdOf("udp drops above 100 per second", series("udp_drops"))).value).toBe(100);
    expect(threshold(thresholdOf("fees over 0.05 SOL", series("fees"))).value).toBe(0.05);
    expect(threshold(thresholdOf("failed more than 1,200", series("failed"))).value).toBe(1200);
  });

  it("turns a value in another unit of the same kind into the series' own", () => {
    expect(threshold(thresholdOf("replay over 1.5 s", series("replay"))).value).toBe(1500);
    expect(threshold(thresholdOf("network in under 10 MB per second", series("net_in"))).value).toBe(10);
    expect(threshold(thresholdOf("network in over 800 mbps", series("net_in"))).value).toBe(100);
    expect(threshold(thresholdOf("egress above 1 gbps", series("net_out"))).value).toBe(125);
    expect(threshold(thresholdOf("validator memory over 256 GiB", series("validator_memory"))).value).toBe(256);
    expect(threshold(thresholdOf("memory over 300 gb", series("memory"))).value).toBeCloseTo(279.4, 1);
    expect(threshold(thresholdOf("tps under 600 per minute", series("transactions"))).value).toBe(10);
    expect(threshold(thresholdOf("tips above 50,000,000 lamports", series("tips"))).value).toBe(0.05);
    expect(threshold(thresholdOf("compute over 40 million", series("compute"))).value).toBe(40);
    expect(threshold(thresholdOf("compute over 48,000,000", series("compute"))).value).toBe(48);
    expect(threshold(thresholdOf("compute above 50000", series("compute"))).value).toBe(0.05);
    expect(threshold(thresholdOf("network out over 500 kbps", series("net_out"))).value).toBeCloseTo(0.0625);
    expect(threshold(thresholdOf("network in over 100 megabits a second", series("net_in"))).value).toBe(12.5);
    expect(threshold(thresholdOf("replay over 20000 microseconds", series("replay"))).value).toBe(20);
    expect(threshold(thresholdOf("replay over 900us", series("replay"))).value).toBe(0.9);
    expect(threshold(thresholdOf("tps over 3k", series("transactions"))).value).toBe(3000);
    expect(threshold(thresholdOf("tps over 3k per second", series("transactions"))).value).toBe(3000);
    expect(threshold(thresholdOf("memory over 300g", series("memory"))).value).toBe(300);
    expect(threshold(thresholdOf("failed over 50 per block", series("failed"))).value).toBe(50);
  });

  it("reads comparing words and signs, with at least and at most counting the value itself", () => {
    expect(threshold(thresholdOf("when replay exceeded 30 ms", series("replay")))).toMatchObject({ above: true, inclusive: false });
    expect(threshold(thresholdOf("replay at least 30 ms", series("replay")))).toMatchObject({ above: true, inclusive: true });
    expect(threshold(thresholdOf("when tps dipped below 40", series("transactions"))).above).toBe(false);
    expect(threshold(thresholdOf("load less than 4", series("load")))).toMatchObject({ above: false, value: 4 });
    expect(threshold(thresholdOf("replay longer than 30 ms", series("replay")))).toMatchObject({ above: true, value: 30 });
    expect(threshold(thresholdOf("replay shorter than 3 ms", series("replay"))).above).toBe(false);
    expect(threshold(thresholdOf("cpu > 80", series("cpu")))).toMatchObject({ above: true, value: 80 });
    expect(threshold(thresholdOf("cpu<=20%", series("cpu")))).toMatchObject({ above: false, inclusive: true, value: 20 });
    expect(threshold(thresholdOf("tps no more than 4000", series("transactions")))).toMatchObject({ above: false, inclusive: true });
    expect(threshold(thresholdOf("load over .5", series("load"))).value).toBe(0.5);
    expect(thresholdOf("the dover cpu 80", series("cpu"))).toBeNull();
  });

  it("takes the comparison after a series' own name where there are several", () => {
    const request = "cpu over 80 and memory over 200 gb";
    expect(threshold(thresholdOf(request, series("cpu"))).value).toBe(80);
    expect(threshold(thresholdOf(request, series("memory"))).value).toBeCloseTo(186.3, 1);
    expect(threshold(thresholdOf("memory over 200 gb and cpu over 80", series("cpu"))).value).toBe(80);
    expect(threshold(thresholdOf("replay over 20 ms and cpu over 80%", series("cpu"))).value).toBe(80);
  });

  it("does not give a series the value after a longer name that holds its own", () => {
    expect(threshold(thresholdOf("failed transactions over 50 and transactions over 3000", series("transactions"))).value).toBe(3000);
    expect(threshold(thresholdOf("failed transactions over 50 and transactions over 3000", series("failed"))).value).toBe(50);
    expect(threshold(thresholdOf("priority fees over 0.1 and fees over 1", series("fees"))).value).toBe(1);
    expect(threshold(thresholdOf("validator memory over 100 and memory over 200", series("memory"))).value).toBe(200);
    expect(threshold(thresholdOf("validator memory over 100 and memory over 200", series("validator_memory"))).value).toBe(100);
  });

  it("takes a number before a span of time for the window, not a value", () => {
    expect(thresholdOf("replay over the past hour", series("replay"))).toBeNull();
    expect(thresholdOf("replay over 2 hours", series("replay"))).toBeNull();
    expect(thresholdOf("cpu over 30 minutes", series("cpu"))).toBeNull();
    expect(thresholdOf("tps over 30 seconds", series("transactions"))).toBeNull();
    expect(threshold(thresholdOf("over 6 hours, when was replay above 20 ms", series("replay"))).value).toBe(20);
  });

  it("says when the value is in a unit the series is not read in", () => {
    expect(thresholdOf("replay over 80 percent", series("replay"))).toEqual({
      wrong: "80 percent does not fit replay time, which is read in ms.",
    });
    expect(thresholdOf("cpu above 20 ms", series("cpu"))).toEqual({ wrong: "20 ms does not fit CPU use, which is read in percent." });
    expect(thresholdOf("failed over 5%", series("failed"))).toEqual({ wrong: "5% does not fit failed transactions, which is read in per block." });
    expect(thresholdOf("transactions over 3000 per block", series("transactions"))).toEqual({
      wrong: "3000 per block does not fit transactions, which is read in per second.",
    });
    expect(thresholdOf("replay over 20xs", series("replay"))).toEqual({ wrong: "20 xs does not fit replay time, which is read in ms." });
  });

  it("is null with no value, and takes a word after the number for no unit", () => {
    expect(thresholdOf("when was replay high", series("replay"))).toBeNull();
    expect(threshold(thresholdOf("tps over 50 in the last hour", series("transactions")))).toMatchObject({
      value: 50,
      matched: "over 50",
    });
  });
});

describe("withoutThreshold", () => {
  it("takes the comparison out, so seconds in it are not read as the window", () => {
    expect(windowOf("when was replay under 5s").millis).toBe(5 * 60_000);
    expect(windowOf(withoutThreshold("when was replay under 5s", series("replay")))).toMatchObject({ given: false });
    expect(windowOf(withoutThreshold("when was replay over 20 ms in the last 2 hours", series("replay"))).millis).toBe(7_200_000);
    expect(windowOf(withoutThreshold("compute over 30m", series("compute"))).millis).toBe(30 * 60_000);
  });

  it("leaves a request with no comparison as it is", () => {
    expect(withoutThreshold("replay over the past hour", series("replay"))).toBe("replay over the past hour");
  });
});

describe("comparing", () => {
  it("finds a comparing word with or without a value", () => {
    expect(comparing("when was replay over")).toBe(true);
    expect(comparing("replay longer than usual")).toBe(true);
    expect(comparing("show replay")).toBe(false);
  });
});

describe("spansOf", () => {
  it("joins runs of spans where the test holds, a span with no reading ending one", () => {
    const spans = spansOf([1, 5, 6, null, 7, 2, 8], 1000, 10, (value) => value > 4);
    expect(spans).toEqual([
      { start: 1010, end: 1030 },
      { start: 1040, end: 1050 },
      { start: 1060, end: 1070 },
    ]);
    expect(spansOf([1, 2], 0, 10, (value) => value > 4)).toEqual([]);
    expect(spansOf([1, 9], 0, 70, (value) => value > 4, 100)).toEqual([{ start: 70, end: 100 }]);
  });
});

describe("lasting", () => {
  it("gives seconds, minutes, or hours and minutes", () => {
    expect(lasting(45_000)).toBe("45 s");
    expect(lasting(7500)).toBe("7.5 s");
    expect(lasting(240_000)).toBe("4 min");
    expect(lasting(5_400_000)).toBe("1 h 30 min");
    expect(lasting(7_200_000)).toBe("2 h");
  });
});

describe("readAs", () => {
  it("says what was worked out", () => {
    expect(readAs("total", series("fees"), null)).toEqual({ head: "Total of fees", unit: "SOL" });
    expect(readAs("mean", series("replay"), null)).toEqual({ head: "Mean of replay time", unit: "ms" });
    expect(readAs("when_above", series("replay"), thresholdOf("over 20 ms", series("replay")))).toEqual({
      head: "When replay time was over 20 ms",
      unit: "",
    });
    expect(readAs("when_above", series("cpu"), thresholdOf("at least 90%", series("cpu"))).head).toBe("When CPU use was at least 90 percent");
    expect(readAs("chart", series("shreds"), null)).toEqual({ head: "Shreds", unit: "per second" });
    expect(readAs("total", series("cpu"), null)).toEqual({ head: "CPU use", unit: "percent" });
  });
});

const T = 1_764_000_000_000;

/** An hour of blocks, one every ten seconds, each with the replay given by `replay`. */
function hour(replay: (index: number) => number, from = T, limit = 20): Bins {
  const rows: [number, WireRow, number][] = [];
  for (let index = 0; index < 360; index += 1) rows.push([from + index * 10_000, row(from + index * 10_000, replay(index), 10), 9000 + index]);
  const bins = bucket(rows, slot("replay"), T, T + 3_600_000, 120, limit);
  if (!bins) throw new Error("no bins");
  return bins;
}

describe("bucket's figures from the blocks", () => {
  it("adds up each block rather than the spans' means", () => {
    // Two blocks in the first span and one in the second: the mean of the means would be 15.
    const rows: [number, WireRow, number][] = [
      [T, row(T, 10_000), 1],
      [T + 1000, row(T + 1000, 10_000), 2],
      [T + 30_000, row(T + 30_000, 25_000), 3],
    ];
    const bins = bucket(rows, slot("replay"), T, T + 3_600_000, 120, 20);
    expect(bins?.blocks).toMatchObject({ total: 45, count: 3, mean: 15, over: 1, under: 2 });
    expect(bins?.blocks?.high).toEqual({ value: 25, time: T + 30_000, slot: 3 });
    expect(bins?.blocks?.low).toEqual({ value: 10, time: T, slot: 1 });
  });

  it("leaves out blocks from outside the window", () => {
    const rows: [number, WireRow, number][] = [
      [T - 5000, row(T - 5000, 90_000), 1],
      [T + 1000, row(T + 1000, 10_000), 2],
      [T + 3_600_000, row(T + 3_600_000, 90_000), 3],
    ];
    expect(bucket(rows, slot("replay"), T, T + 3_600_000)?.blocks).toMatchObject({ total: 10, count: 1 });
  });

  it("knows when its oldest block was taken", () => {
    expect(hour(() => 5000).first).toBe(T);
    expect(hour(() => 5000, T + 1_800_000).first).toBe(T + 1_800_000);
  });
});

describe("answerFigure", () => {
  it("gives nothing for a chart or a share, which the card works out", () => {
    expect(answerFigure("chart", series("replay"), hour(() => 5000), context())).toBeNull();
    expect(answerFigure("share", series("failed"), hour(() => 5000), context())).toBeNull();
    expect(answerFigure(null, series("replay"), hour(() => 5000), context())).toBeNull();
  });

  it("totals the blocks and says the fees are every leader's", () => {
    const rows: [number, WireRow, number][] = [
      [T, row(T, 1), 1],
      [T + 10_000, row(T + 10_000, 1), 2],
    ];
    const fees = bucket(rows, slot("fees"), T, T + 3_600_000);
    expect(answerFigure("total", series("fees"), fees, context())).toEqual({
      label: "Total fees, past hour",
      value: "6 SOL",
      detail: "2 blocks from every leader",
      warn: false,
    });
    const compute = bucket(rows, slot("compute"), T, T + 3_600_000);
    expect((answerFigure("total", series("compute"), compute, context()) as { value: string }).value).toBe("0.08 billion CU");
  });

  it("refuses a total that means nothing", () => {
    expect(answerFigure("total", series("replay"), hour(() => 5000), context())).toEqual({
      unfit: "A total of replay time is not meaningful.",
    });
  });

  it("gives a per-block mean and the single highest block with its slot", () => {
    const bins = hour((index) => (index === 100 ? 412_000 : 8000));
    expect(answerFigure("mean", series("replay"), bins, context())).toMatchObject({ label: "Mean replay time, past hour", detail: "360 blocks" });
    expect((answerFigure("mean", series("replay"), bins, context()) as { value: string }).value).toMatch(/ ms per block$/);
    expect(answerFigure("highest", series("replay"), bins, context())).toEqual({
      label: "Highest replay time, past hour",
      value: "412 ms",
      detail: `slot 9,100 at t${T + 1_000_000}`,
      warn: false,
    });
  });

  it("names the real span of a window the history only partly holds", () => {
    const bins = hour(() => 8000, T + 1_800_000);
    const answer = answerFigure("mean", series("replay"), bins, context()) as { label: string; detail: string; warn: boolean };
    expect(answer.label).toBe(`Mean replay time, since t${T + 1_800_000}`);
    expect(answer.detail).toBe("180 blocks · the history holds 30 min of the past hour");
    expect(answer.warn).toBe(true);
  });

  it("finds when a series was over a value, as spans and a count of blocks", () => {
    // Fifteen minutes from minute 20 at 30 ms, the rest at 8 ms.
    const bins = hour((index) => (index >= 120 && index < 210 ? 30_000 : 8000));
    const over = threshold(thresholdOf("over 20 ms", series("replay")));
    const answer = answerFigure("when_above", series("replay"), bins, context({ threshold: over }));
    expect(answer).toMatchObject({
      label: "Over 20 ms in 1 span, 15 min in all",
      detail: `longest t${T + 1_200_000}–t${T + 2_100_000} · by 30 s averages · 90 of 360 blocks were over 20 ms`,
      mark: { threshold: 20, spans: [{ start: T + 1_200_000, end: T + 2_100_000 }] },
    });
  });

  it("says a series was never over the value, and gives its highest", () => {
    const bins = hour(() => 8000);
    const answer = answerFigure("when_above", series("replay"), bins, context({ threshold: threshold(thresholdOf("over 20 ms", series("replay"))) }));
    expect(answer).toMatchObject({ label: "Never over 20 ms in the past hour", detail: "highest 30 s average 8 ms · 0 of 360 blocks were over 20 ms" });
  });

  it("asks for a value only where the request compared without one", () => {
    expect(answerFigure("when_above", series("replay"), hour(() => 8000), context({ comparing: true }))).toEqual({
      unfit: 'No value to compare against; ask, for example, "over 20 ms".',
    });
    expect(answerFigure("when_above", series("replay"), hour(() => 8000), context())).toBeNull();
  });

  it("passes on a value in the wrong unit", () => {
    const wrong = thresholdOf("replay over 80 percent", series("replay"));
    expect(answerFigure("when_above", series("replay"), hour(() => 8000), context({ threshold: wrong }))).toEqual({
      unfit: "80 percent does not fit replay time, which is read in ms.",
    });
  });

  it("says no average was over where a single block was, rather than never", () => {
    const bins = hour((index) => (index === 100 ? 412_000 : 8000), T, 200);
    const answer = answerFigure("when_above", series("replay"), bins, context({ threshold: threshold(thresholdOf("over 200 ms", series("replay"))) }));
    expect(answer).toMatchObject({ label: "No 30 s average over 200 ms in the past hour" });
    expect((answer as { detail: string }).detail).toContain("1 of 360 blocks was over 200 ms");
    // Counted against another value, the highest block still keeps it from "never".
    const other = answerFigure("when_above", series("replay"), hour((index) => (index === 100 ? 412_000 : 8000)), context({ threshold: threshold(thresholdOf("over 200 ms", series("replay"))) }));
    expect(other).toMatchObject({ label: "No 30 s average over 200 ms in the past hour" });
    expect((other as { detail: string }).detail).not.toContain("blocks");
  });

  it("counts the blocks only against the value they were counted against", () => {
    const bins = hour(() => 8000);
    const answer = answerFigure("when_below", series("replay"), bins, context({ threshold: threshold(thresholdOf("under 5 ms", series("replay"))) }));
    expect(answer).toMatchObject({ label: "Never under 5 ms in the past hour", detail: "lowest 30 s average 8 ms" });
  });

  it("gives the longest of several spans and when below a value", () => {
    const bins = hour((index) => (index < 12 || (index >= 60 && index < 90) ? 2000 : 8000));
    const answer = answerFigure("when_below", series("replay"), bins, context({ threshold: threshold(thresholdOf("under 5 ms", series("replay"))) }));
    expect(answer).toMatchObject({ label: "Under 5 ms in 2 spans, 7 min in all" });
    expect((answer as { detail: string }).detail).toContain(`longest t${T + 600_000}–t${T + 900_000}`);
  });

  /** An hour of a rate series beginning at `start`, `perBlock` each ten seconds. */
  function rate(perBlock: number, start = T): Bins {
    const rows: [number, WireRow, number][] = [];
    for (let time = start; time < T + 3_600_000; time += 10_000) rows.push([time, row(time, 1, 1, perBlock), 1]);
    const bins = bucket(rows, slot("transactions"), T, T + 3_600_000, 120);
    if (!bins) throw new Error("no bins");
    return bins;
  }

  it("totals a rate as a count and gives its mean, highest and lowest per second", () => {
    const bins = rate(2500);
    expect(answerFigure("total", series("transactions"), bins, context())).toMatchObject({ value: "900,000 transactions", detail: "360 blocks" });
    expect(answerFigure("mean", series("transactions"), bins, context())).toMatchObject({ value: "250 per second" });
    expect(answerFigure("highest", series("transactions"), bins, context())).toMatchObject({ value: "250 per second" });
    expect((answerFigure("lowest", series("transactions"), bins, context()) as { detail: string }).detail).toMatch(/^30 s average, at /);
  });

  it("leaves a rate's span the history began in out of its lowest and mean", () => {
    // Blocks from four seconds before the end of a span: that span reads a seventh of the rest.
    const bins = rate(2500, T + 1_800_000 - 4000);
    expect(answerFigure("lowest", series("transactions"), bins, context())).toMatchObject({ value: "250 per second" });
    expect(answerFigure("mean", series("transactions"), bins, context())).toMatchObject({ value: "250 per second" });
  });

  it("leaves a rate's part-filled spans out of when it was under a value", () => {
    const begun = rate(2500, T + 1_800_000 - 4000);
    const under = threshold(thresholdOf("tps under 100", series("transactions")));
    expect(answerFigure("when_below", series("transactions"), begun, context({ threshold: under }))).toMatchObject({
      label: `Never under 100 per second since t${T + 1_800_000 - 4000}`,
    });
    // A gap of a minute in the middle: the spans either side held blocks for part of their seconds.
    const rows: [number, WireRow, number][] = [];
    for (let time = T; time < T + 3_600_000; time += 10_000) {
      if (time < T + 1_204_000 || time >= T + 1_264_000) rows.push([time, row(time, 1, 1, 2500), 1]);
    }
    const gapped = bucket(rows, slot("transactions"), T, T + 3_600_000, 120);
    expect(answerFigure("when_below", series("transactions"), gapped, context({ threshold: under }))).toMatchObject({
      label: "Never under 100 per second in the past hour",
    });
  });

  it("counts blocks at least a value only where they were counted that way", () => {
    const rows: [number, WireRow, number][] = [];
    for (let index = 0; index < 360; index += 1) rows.push([T + index * 10_000, row(T + index * 10_000, 8000), index]);
    const strict = bucket(rows, slot("replay"), T, T + 3_600_000, 120, 8);
    const atLeast = threshold(thresholdOf("at least 8 ms", series("replay")));
    expect((answerFigure("when_above", series("replay"), strict, context({ threshold: atLeast })) as { detail: string }).detail).not.toContain("blocks");
    const inclusive = bucket(rows, slot("replay"), T, T + 3_600_000, 120, 8, true);
    expect((answerFigure("when_above", series("replay"), inclusive, context({ threshold: atLeast })) as { detail: string }).detail).toContain(
      "360 of 360 blocks were at least 8 ms",
    );
  });

  it("does not say the history began inside the window over a few skipped slots", () => {
    const rows: [number, WireRow, number][] = [];
    for (let time = T + 3200; time < T + 300_000; time += 400) rows.push([time, row(time, 8000), 1]);
    const bins = bucket(rows, slot("replay"), T, T + 300_000);
    expect((answerFigure("mean", series("replay"), bins, context({ window: "past 5 minutes" })) as { warn: boolean }).warn).toBe(false);
  });

  const host = (values: (number | null)[], peaks: (number | null)[] = values, step = 30_000): Bins => ({
    from: T,
    to: T + values.length * step,
    values,
    min: Math.min(...values.filter((value): value is number => value !== null)),
    max: Math.max(...values.filter((value): value is number => value !== null)),
    mean: 0,
    latest: 0,
    binMillis: step,
    first: T + values.findIndex((value) => value !== null) * step,
    peaks,
  });

  it("totals a host count over each step's seconds, and network in gigabytes", () => {
    expect((answerFigure("total", series("udp_drops"), host([2, 4, 6]), context()) as { value: string }).value).toBe("360 datagrams");
    expect((answerFigure("total", series("net_in"), host([100, 100]), context()) as { value: string }).value).toBe("6 GB");
    expect((answerFigure("total", series("net_in"), host([100, null, 100]), context()) as { value: string }).value).toBe("about 6 GB");
  });

  it("gives a host's one-second peak as its highest, and the step it fell in", () => {
    const answer = answerFigure("highest", series("cpu"), host([40, 50, 45], [60, 93, 70]), context());
    expect(answer).toMatchObject({ value: "93 percent", detail: `one-second reading, in the 30 s from t${T + 30_000}` });
  });

  it("falls back to the highest step without peaks, and gives the lowest step and the mean", () => {
    const bins = { ...host([40, 50, 45]), peaks: undefined };
    expect(answerFigure("highest", series("cpu"), bins, context())).toMatchObject({ value: "50 percent", detail: `30 s step, at t${T + 30_000}` });
    expect(answerFigure("lowest", series("cpu"), host([40, 50, 45]), context())).toMatchObject({ value: "40 percent", detail: `30 s step, at t${T}` });
    expect(answerFigure("mean", series("cpu"), host([40, 50, 45]), context())).toMatchObject({ value: "45 percent", detail: "from readings in 30 s steps" });
  });

  it("counts the host steps whose one-second reading crossed when no step's mean did", () => {
    const answer = answerFigure("when_above", series("cpu"), host([40, 50, 45], [60, 93, 70]), context({ threshold: threshold(thresholdOf("over 80%", series("cpu"))) }));
    expect(answer).toMatchObject({ label: "No 30 s step over 80 percent in the past hour" });
    expect((answer as { detail: string }).detail).toContain("1 step had a one-second reading over 80 percent");
  });

  it("names the real span of a host window the readings only partly hold", () => {
    const answer = answerFigure("mean", series("cpu"), host([null, null, 40, 50]), context()) as { label: string; warn: boolean };
    expect(answer.label).toBe(`Mean CPU use, since t${T + 60_000}`);
    expect(answer.warn).toBe(true);
  });
});

describe("shareFigure", () => {
  const rows: [number, WireRow, number][] = [
    [T, row(T, 1, 3, 100), 1],
    [T + 10_000, row(T + 10_000, 1, 1, 300), 2],
  ];
  const failed = bucket(rows, slot("failed"), T, T + 3_600_000);
  const transactions = bucket(rows, slot("transactions"), T, T + 3_600_000);

  it("gives the part's total over the whole's", () => {
    const pair = sharePair([series("failed")]);
    expect(shareFigure(pair, failed, transactions, context())).toEqual({
      label: "Failed transactions as a share of transactions, past hour",
      value: "1%",
      detail: "4 of 400 · transactions taken as the whole",
      warn: false,
    });
  });

  it("says when a share is not worked out, or the whole is nought", () => {
    expect(shareFigure(null, failed, transactions, context())).toEqual({ unfit: NO_SHARE });
    const none = bucket([[T, row(T, 1, 0, 0), 1]], slot("transactions"), T, T + 3_600_000);
    expect(shareFigure(sharePair([series("failed")]), failed, none, context())).toEqual({ unfit: "No transactions in the window." });
  });
});
