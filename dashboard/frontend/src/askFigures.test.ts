import { describe, expect, it } from "vitest";
import { bucket, SERIES, windowOf, type Bins, type Series, type SlotSeries } from "./ask";
import {
  answerFigure,
  comparing,
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
  windowMillis: 3_600_000,
  threshold: null,
  comparing: false,
  clock: (millis) => `t${millis}`,
  ...overrides,
});

describe("the operation prompt", () => {
  it("is the text measured on the built-in model", () => {
    expect(OPERATION_PROMPT).toHaveLength(527);
    expect(OPERATION_PROMPT.split("\n")[0]).toBe(
      "A request asks about things recorded about a Solana validator and the machine it runs on. Answer the operation it asks for.",
    );
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
    expect(threshold(thresholdOf("compute over 40 m", series("compute"))).value).toBe(40);
    expect(threshold(thresholdOf("compute over 48,000,000", series("compute"))).value).toBe(48);
  });

  it("reads every comparing word, from under and below to exceeded and at least", () => {
    expect(threshold(thresholdOf("when replay exceeded 30 ms", series("replay"))).above).toBe(true);
    expect(threshold(thresholdOf("replay at least 30 ms", series("replay"))).above).toBe(true);
    expect(threshold(thresholdOf("when tps dipped below 40", series("transactions"))).above).toBe(false);
    expect(threshold(thresholdOf("load less than 4", series("load")))).toMatchObject({ above: false, value: 4 });
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
      wrong: "80 percent is not a unit replay time is read in.",
    });
    expect(thresholdOf("cpu above 20 ms", series("cpu"))).toEqual({ wrong: "20 ms is not a unit CPU use is read in." });
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
  });

  it("leaves a request with no comparison as it is", () => {
    expect(withoutThreshold("replay over the past hour", series("replay"))).toBe("replay over the past hour");
  });
});

describe("comparing", () => {
  it("finds a comparing word with or without a value", () => {
    expect(comparing("when was replay over")).toBe(true);
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
  });
});

describe("lasting", () => {
  it("gives seconds, minutes, or hours and minutes", () => {
    expect(lasting(45_000)).toBe("45 s");
    expect(lasting(240_000)).toBe("4 min");
    expect(lasting(5_400_000)).toBe("1 h 30 min");
    expect(lasting(7_200_000)).toBe("2 h");
  });
});

describe("readAs", () => {
  it("says what was worked out", () => {
    expect(readAs("total", series("fees"), null)).toEqual({ head: "Total of Fees", unit: "SOL" });
    expect(readAs("mean", series("replay"), null)).toEqual({ head: "Mean of Replay time", unit: "ms" });
    expect(readAs("when_above", series("replay"), thresholdOf("over 20 ms", series("replay")))).toEqual({
      head: "When Replay time was over 20 ms",
      unit: "",
    });
    expect(readAs("chart", series("shreds"), null)).toEqual({ head: "Shreds", unit: "per second" });
    expect(readAs("total", series("cpu"), null)).toEqual({ head: "CPU use", unit: "percent" });
  });
});

const T = 1_764_000_000_000;

/** An hour of blocks, one every ten seconds, each with the replay given by `replay`. */
function hour(replay: (index: number) => number, from = T): Bins {
  const rows: [number, WireRow, number][] = [];
  for (let index = 0; index < 360; index += 1) rows.push([from + index * 10_000, row(from + index * 10_000, replay(index), 10), 9000 + index]);
  const bins = bucket(rows, slot("replay"), T, T + 3_600_000, 120, 20);
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
      detail: `longest t${T + 1_200_000}–t${T + 2_100_000} · by 30 s averages · 90 of 360 blocks were over`,
      mark: { threshold: 20, spans: [{ start: T + 1_200_000, end: T + 2_100_000 }] },
    });
  });

  it("says a series was never over the value, and gives its highest", () => {
    const bins = hour(() => 8000);
    const answer = answerFigure("when_above", series("replay"), bins, context({ threshold: threshold(thresholdOf("over 20 ms", series("replay"))) }));
    expect(answer).toMatchObject({ label: "Never over 20 ms in the past hour", detail: "highest 30 s average 8 · 0 of 360 blocks were over" });
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
      unfit: "80 percent is not a unit replay time is read in.",
    });
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
