import { describe, expect, it } from "vitest";
import {
  asksWhy,
  bucket,
  ceiling,
  figure,
  MAX_EMPTY_PAGES,
  MAX_PAGES,
  MAX_WINDOW_MILLIS,
  MIN_WINDOW_MILLIS,
  namesSeries,
  NONE,
  parseReply,
  paths,
  readWindow,
  recordedList,
  SERIES,
  seriesSchema,
  systemPrompt,
  windowLabel,
  windowOf,
  windowShort,
  type Series,
} from "./ask";
import {
  HAS_BLOCK,
  HAS_CLOCK,
  HAS_REPLAY,
  HAS_SHREDS,
  HAS_TIPS,
  type SlotRange,
  type WireRow,
} from "./slotHistory";

const MINUTE = 60_000;
const HOUR = 3_600_000;

function series(key: string): Series {
  const found = SERIES.find((entry) => entry.key === key);
  if (!found) throw new Error(`no series ${key}`);
  return found;
}

/** A row with every column distinct, so a wrong index shows. */
function row(flags: number, time = 0): WireRow {
  return [4, flags, 2000, 1500, 40_000_000, 3_000_000_000, 1_000_000_000, 500_000_000, time, 9000, 1300, 7, 380, 395, 12, 31, 180];
}

describe("windowOf", () => {
  it("reads an amount and a unit however it is written", () => {
    expect(windowOf("shreds over the past 6 hours").millis).toBe(6 * HOUR);
    expect(windowOf("replay time, last 15 minutes").millis).toBe(15 * MINUTE);
    expect(windowOf("tps 90 min").millis).toBe(90 * MINUTE);
    expect(windowOf("fees 2h").millis).toBe(2 * HOUR);
    expect(windowOf("fees for 45m").millis).toBe(45 * MINUTE);
    expect(windowOf("Tips over the last 3 hrs").millis).toBe(3 * HOUR);
    expect(windowOf("votes in a 30-minute window").millis).toBe(30 * MINUTE);
  });

  it("reads a decimal amount whole", () => {
    expect(windowOf("shreds over the last 1.5 hours").millis).toBe(90 * MINUTE);
  });

  it("reads number words and a bare unit", () => {
    expect(windowOf("shreds over the past hour").millis).toBe(HOUR);
    expect(windowOf("shreds over the last two hours").millis).toBe(2 * HOUR);
    expect(windowOf("shreds for an hour").millis).toBe(HOUR);
    expect(windowOf("shreds in the last hour").millis).toBe(HOUR);
    expect(windowOf("shreds over ten minutes").millis).toBe(10 * MINUTE);
    expect(windowOf("shreds, last twenty minutes").millis).toBe(20 * MINUTE);
    expect(windowOf("shreds, last twenty-five minutes").millis).toBe(25 * MINUTE);
    expect(windowOf("shreds over forty five minutes").millis).toBe(45 * MINUTE);
  });

  it("reads the named fractions of an hour", () => {
    expect(windowOf("failed transactions, last half hour").millis).toBe(30 * MINUTE);
    expect(windowOf("failed transactions over half an hour").millis).toBe(30 * MINUTE);
    expect(windowOf("compute for an hour and a half").millis).toBe(90 * MINUTE);
    expect(windowOf("one and a half hours of fees").millis).toBe(90 * MINUTE);
    expect(windowOf("two and a half hours of fees").millis).toBe(150 * MINUTE);
    expect(windowOf("fees over 3 and a half hours").millis).toBe(210 * MINUTE);
    expect(windowOf("compute, last quarter of an hour").millis).toBe(15 * MINUTE);
  });

  it("falls back to an hour and says the request named none", () => {
    expect(windowOf("show me shreds per second")).toEqual({ millis: HOUR, given: false, clamped: false });
  });

  it("holds the window to what the page draws and says so", () => {
    expect(windowOf("shreds over the past day")).toEqual({ millis: MAX_WINDOW_MILLIS, given: true, clamped: true });
    expect(windowOf("shreds over the last 24 hours").clamped).toBe(true);
    expect(windowOf("shreds over the last 2 minutes")).toEqual({ millis: MIN_WINDOW_MILLIS, given: true, clamped: true });
    expect(windowOf("shreds over the last 6 hours").clamped).toBe(false);
    expect(windowOf("shreds over the last 30 seconds")).toEqual({ millis: MIN_WINDOW_MILLIS, given: true, clamped: true });
    expect(windowOf("shreds over the past week")).toEqual({ millis: MAX_WINDOW_MILLIS, given: true, clamped: true });
  });

  it("does not read milliseconds or a slot number as a window", () => {
    expect(windowOf("replay time over 100 ms").given).toBe(false);
    expect(windowOf("shreds around slot 381,204,117").given).toBe(false);
    expect(windowOf("shreds around slot 381204117 holding").given).toBe(false);
  });

  it("does not read a word that only starts with a unit", () => {
    expect(windowOf("show a metric").given).toBe(false);
    expect(windowOf("a drop in shreds").given).toBe(false);
  });

  it("does not read a unit's letter out of a word or after a space", () => {
    expect(windowOf("fees and tips over the last 15 minutes").millis).toBe(15 * MINUTE);
    expect(windowOf("shreds since 9 am").given).toBe(false);
    expect(windowOf("how am I doing on votes").given).toBe(false);
    expect(windowOf("an ad for fees").given).toBe(false);
    expect(windowOf("compute over 40 m cu").given).toBe(false);
  });

  it("reads a count per unit as a rate, not a window", () => {
    expect(windowOf("transactions a minute").given).toBe(false);
    expect(windowOf("shreds an hour over the last 2 hours").millis).toBe(2 * HOUR);
    expect(windowOf("votes in the second half of the turn").given).toBe(false);
  });
});

describe("asksWhy", () => {
  it("catches a request for a cause", () => {
    expect(asksWhy("Why was slot 381,204,117 skipped?")).toBe(true);
    expect(asksWhy("explain the drop in shreds")).toBe(true);
    expect(asksWhy("what caused the replay spike")).toBe(true);
    expect(asksWhy("What factors affected us skipping that slot")).toBe(true);
    expect(asksWhy("how come tps fell")).toBe(true);
    expect(asksWhy("the reason for the missed votes")).toBe(true);
  });

  it("lets a chart request through, even one that starts with what", () => {
    expect(asksWhy("What were shreds per second over the past hour")).toBe(false);
    expect(asksWhy("Show me failed transactions")).toBe(false);
    expect(asksWhy("show reasonable fees")).toBe(false);
    expect(asksWhy("failed transactions by reason")).toBe(false);
    expect(asksWhy("chart what happened to shreds over the past hour")).toBe(false);
  });
});

describe("SERIES", () => {
  it("has keys that are unique and never the model's word for no match", () => {
    const keys = SERIES.map((entry) => entry.key);
    expect(new Set(keys).size).toBe(keys.length);
    expect(keys).not.toContain(NONE);
  });

  it("reads nothing from a row without the flag its column needs", () => {
    for (const entry of SERIES) expect(entry.read(row(HAS_CLOCK))).toBeNull();
  });

  it("reads each column, scaled to its unit", () => {
    const full = row(HAS_BLOCK | HAS_CLOCK | HAS_TIPS | HAS_REPLAY | HAS_SHREDS);
    expect(series("shreds").read(full)).toBe(1300);
    expect(series("repaired").read(full)).toBe(7);
    expect(series("recovered").read(full)).toBe(31);
    expect(series("transactions").read(full)).toBe(1500);
    expect(series("votes").read(full)).toBe(2000);
    expect(series("failed").read(full)).toBe(180);
    expect(series("compute").read(full)).toBe(40);
    expect(series("fees").read(full)).toBe(3);
    expect(series("priority_fees").read(full)).toBe(1);
    expect(series("tips").read(full)).toBe(0.5);
    expect(series("replay").read(full)).toBe(9);
    expect(series("fill").read(full)).toBe(380);
  });

  it("reads tips only where the block's tips were read", () => {
    expect(series("tips").read(row(HAS_BLOCK))).toBeNull();
    expect(series("tips").read(row(HAS_TIPS))).toBeNull();
  });
});

describe("namesSeries", () => {
  it("finds a series' own words in a request", () => {
    expect(namesSeries("Shreds per second over the past hour", series("shreds"))).toBe(true);
    expect(namesSeries("show TPS", series("transactions"))).toBe(true);
    expect(namesSeries("how many CUs per block", series("compute"))).toBe(true);
    expect(namesSeries("voting transactions", series("votes"))).toBe(true);
  });

  it("does not find them inside another word or in an unrelated request", () => {
    expect(namesSeries("GPU temperature", series("compute"))).toBe(false);
    expect(namesSeries("calculate the load", series("compute"))).toBe(false);
    expect(namesSeries("network traffic", series("shreds"))).toBe(false);
  });
});

describe("seriesSchema", () => {
  it("allows each key and the word for no match, and nothing open", () => {
    expect(seriesSchema(SERIES)).toEqual({
      type: "object",
      properties: { series: { type: "string", enum: [...SERIES.map((entry) => entry.key), NONE] } },
      required: ["series"],
      additionalProperties: false,
    });
  });
});

describe("systemPrompt", () => {
  it("names every key and stays small beside the model's context", () => {
    const prompt = systemPrompt(SERIES);
    for (const entry of SERIES) expect(prompt).toContain(`${entry.key}: ${entry.hint}`);
    expect(prompt).toContain(NONE);
    expect(prompt.length).toBeLessThan(2000);
  });
});

describe("parseReply", () => {
  it("returns the series the model named", () => {
    expect(parseReply('{"series":"replay"}', SERIES)).toBe(series("replay"));
    expect(parseReply(' {"series": "shreds"}\n', SERIES)).toBe(series("shreds"));
  });

  it("returns the word for no match as it is", () => {
    expect(parseReply('{"series":"none"}', SERIES)).toBe(NONE);
  });

  it("returns null for anything outside the schema", () => {
    expect(parseReply('{"series":"cpu"}', SERIES)).toBeNull();
    expect(parseReply('{"chart":"shreds"}', SERIES)).toBeNull();
    expect(parseReply("shreds", SERIES)).toBeNull();
    expect(parseReply('The series is {"series":"shreds"}', SERIES)).toBeNull();
    expect(parseReply("null", SERIES)).toBeNull();
    expect(parseReply("", SERIES)).toBeNull();
  });
});

describe("recordedList", () => {
  it("lists every series by its label", () => {
    expect(recordedList(SERIES.slice(0, 3))).toBe("shreds, repaired shreds, recovered shreds");
  });
});

describe("windowLabel and windowShort", () => {
  it("name a window in whole hours or in minutes", () => {
    expect(windowLabel(HOUR)).toBe("past hour");
    expect(windowLabel(6 * HOUR)).toBe("past 6 hours");
    expect(windowLabel(90 * MINUTE)).toBe("past 90 minutes");
    expect(windowShort(2 * HOUR)).toBe("2 hours");
    expect(windowShort(HOUR)).toBe("1 hour");
    expect(windowShort(45 * MINUTE)).toBe("45 min");
  });
});

/** Slots a second apart from time zero, every one clocked unless `bare` holds it. */
function history(bare: (slot: number) => boolean = () => false) {
  const asked: [number, number][] = [];
  const range = (first: number, count: number): Promise<SlotRange> => {
    asked.push([first, count]);
    const rows = Array.from({ length: count }, (_, index) => {
      const slot = first + index;
      return bare(slot) ? null : row(HAS_CLOCK | HAS_SHREDS, slot * 1000);
    });
    return Promise.resolve({ first_slot: first, rows });
  };
  return { asked, range };
}

describe("readWindow", () => {
  it("stops at the first row from before the window", async () => {
    const { asked, range } = history();
    const read = await readWindow(range, 9999, 9_000_000);
    expect(asked).toEqual([[5904, 4096]]);
    expect(read.rows).toHaveLength(1000);
    expect(read.rows.every(([time]) => time >= 9_000_000)).toBe(true);
    expect(read.reached).toBe(true);
  });

  it("walks back a page at a time until the window is covered", async () => {
    const { asked, range } = history();
    const read = await readWindow(range, 9999, 4_000_000);
    expect(asked).toEqual([
      [5904, 4096],
      [1808, 4096],
    ]);
    expect(read.rows).toHaveLength(6000);
  });

  it("skips slots with no row", async () => {
    const { range } = history((slot) => slot % 2 === 0);
    const read = await readWindow(range, 9999, 9_000_000);
    expect(read.rows).toHaveLength(500);
  });

  it("reads on past a run of slots with no row, as a restart leaves", async () => {
    // Two whole pages bare between the newest rows and the older ones.
    const { asked, range } = history((slot) => slot >= 11_000 && slot < 20_000);
    const read = await readWindow(range, 24_000, 10_000_000);
    expect(asked).toHaveLength(4);
    expect(read.rows).toHaveLength(4001 + 1000);
    expect(read.reached).toBe(true);
  });

  it("takes the history as ended after its most pages with no row, and says so", async () => {
    const { asked, range } = history((slot) => slot < 96_000);
    const read = await readWindow(range, 99_999, 0);
    expect(asked).toHaveLength(1 + MAX_EMPTY_PAGES);
    expect(read.rows).toHaveLength(4000);
    expect(read.reached).toBe(false);
  });

  it("stops at slot nought", async () => {
    const { asked, range } = history();
    const read = await readWindow(range, 99, -1);
    expect(asked).toEqual([[0, 100]]);
    expect(read).toMatchObject({ reached: false });
    expect(read.rows).toHaveLength(100);
  });

  it("gives up after its most pages", async () => {
    const { asked, range } = history();
    const read = await readWindow(range, 10_000_000, -1);
    expect(asked).toHaveLength(MAX_PAGES);
    expect(read.reached).toBe(false);
  });
});

describe("bucket", () => {
  const timed = (time: number): [number, WireRow] => [time, row(HAS_BLOCK | HAS_SHREDS | HAS_CLOCK, time)];

  it("turns a rate into readings per second of each span", () => {
    // Four spans of ten seconds; two blocks of 1300 shreds in the first.
    const bins = bucket([timed(1000), timed(5000)], series("shreds"), 0, 40_000, 4);
    expect(bins?.values).toEqual([260, null, null, null]);
  });

  it("averages a per-block series over the blocks in each span", () => {
    const bins = bucket([timed(1000), timed(5000), timed(12_000)], series("failed"), 0, 40_000, 4);
    expect(bins?.values).toEqual([180, 180, null, null]);
  });

  it("leaves the newest span out, which is still filling", () => {
    const bins = bucket([timed(1000), timed(39_000)], series("shreds"), 0, 40_000, 4);
    expect(bins?.values).toEqual([130, null, null, null]);
  });

  it("holds a reading outside the window to the nearest span", () => {
    const bins = bucket([timed(-5000), timed(15_000)], series("failed"), 0, 40_000, 4);
    expect(bins?.values).toEqual([180, 180, null, null]);
  });

  it("gives the lowest, highest, mean and latest of the spans drawn", () => {
    const light: [number, WireRow] = [15_000, [...row(HAS_BLOCK, 15_000)] as WireRow];
    light[1][16] = 20;
    const bins = bucket([timed(1000), light], series("failed"), 0, 40_000, 4);
    expect(bins).toMatchObject({ from: 0, to: 40_000, min: 20, max: 180, mean: 100, latest: 20 });
  });

  it("is null when no row in the window carries the series", () => {
    expect(bucket([], series("shreds"), 0, 40_000, 4)).toBeNull();
    expect(bucket([[1000, row(HAS_CLOCK, 1000)]], series("shreds"), 0, 40_000, 4)).toBeNull();
  });
});

describe("figure", () => {
  it("keeps about three figures at any size", () => {
    expect(figure(2554.4)).toBe("2,554");
    expect(figure(13.94)).toBe("13.9");
    expect(figure(6.716)).toBe("6.72");
    expect(figure(0.01234)).toBe("0.012");
    expect(figure(0)).toBe("0");
  });
});

describe("ceiling", () => {
  it("is the next round figure at or above the highest reading", () => {
    expect(ceiling(2425)).toBe(2500);
    expect(ceiling(4035)).toBe(5000);
    expect(ceiling(13.9)).toBe(20);
    expect(ceiling(0.42)).toBe(0.5);
    expect(ceiling(100)).toBe(100);
    expect(ceiling(0)).toBe(1);
  });
});

describe("paths", () => {
  it("draws one line and a closed area through a run of readings", () => {
    expect(paths([0, 50, 100], 100, 200, 100)).toEqual({
      line: "M0.0,100.0L100.0,50.0L200.0,0.0",
      area: "M0.0,100L0.0,100.0L100.0,50.0L200.0,0.0L200.0,100Z",
    });
  });

  it("breaks both where a span has no reading", () => {
    const { line, area } = paths([50, 50, null, 100, 100], 100, 400, 100);
    expect(line).toBe("M0.0,50.0L100.0,50.0M300.0,0.0L400.0,0.0");
    expect(area).toBe("M0.0,100L0.0,50.0L100.0,50.0L100.0,100ZM300.0,100L300.0,0.0L400.0,0.0L400.0,100Z");
  });

  it("gives a reading alone between gaps a line of its own", () => {
    expect(paths([null, 50, null], 100, 200, 100).line).toBe("M100.0,50.0L100.0,50.0");
    expect(paths([50], 100, 200, 100).line).toBe("M0.0,50.0L0.0,50.0");
  });

  it("is empty with no readings", () => {
    expect(paths([null, null], 100, 200, 100)).toEqual({ line: "", area: "" });
  });
});
