/** The Ask page's own logic: what can be charted, how a typed request is read, and how the slot
 *  history and the host's readings become charts. The browser's model only picks the series. */

import { HAS_BLOCK, HAS_CLOCK, HAS_REPLAY, HAS_SHREDS, HAS_TIPS, type SlotRange, type WireRow } from "./slotHistory";

interface Named {
  key: string;
  label: string;
  unit: string;
  /** What the model is told the series is. */
  hint: string;
  /** Word stems a request for this series usually holds, its own name first. */
  stems: readonly string[];
}

/** A column of the slot history, one reading a block. */
export interface SlotSeries extends Named {
  source: "slots";
  /** Summed over a bin and divided by its seconds, rather than averaged per block. */
  rate: boolean;
  read: (row: WireRow) => number | null;
}

/** A reading of the machine, kept by the server in ten-second bins. */
export interface HostSeries extends Named {
  source: "host";
  /** Turns the server's figure into the unit shown. */
  scale: number;
}

export type Series = SlotSeries | HostSeries;

const has = (row: WireRow, flags: number): boolean => (row[1] & flags) === flags;
const LAMPORTS_PER_SOL = 1e9;

export const SLOT_SERIES: readonly SlotSeries[] = [
  {
    source: "slots",
    key: "shreds",
    label: "Shreds",
    unit: "per second",
    rate: true,
    hint: "shreds received, per second",
    stems: ["shred"],
    read: (row) => (has(row, HAS_SHREDS) ? row[10] : null),
  },
  {
    source: "slots",
    key: "repaired",
    label: "Repaired shreds",
    unit: "per block",
    rate: false,
    hint: "shreds fetched through repair, per block",
    stems: ["repair"],
    read: (row) => (has(row, HAS_SHREDS) ? row[11] : null),
  },
  {
    source: "slots",
    key: "recovered",
    label: "Recovered shreds",
    unit: "per block",
    rate: false,
    hint: "shreds rebuilt from erasure coding, per block",
    stems: ["recover", "erasure", "rebuil"],
    read: (row) => (has(row, HAS_SHREDS) ? row[15] : null),
  },
  {
    source: "slots",
    key: "transactions",
    label: "Transactions",
    unit: "per second",
    rate: true,
    hint: "user transactions, not votes, per second; TPS",
    stems: ["transaction", "tps\\b", "txs?\\b"],
    read: (row) => (has(row, HAS_BLOCK) ? row[3] : null),
  },
  {
    source: "slots",
    key: "votes",
    label: "Vote transactions",
    unit: "per second",
    rate: true,
    hint: "vote transactions, per second",
    stems: ["vot"],
    read: (row) => (has(row, HAS_BLOCK) ? row[2] : null),
  },
  {
    source: "slots",
    key: "failed",
    label: "Failed transactions",
    unit: "per block",
    rate: false,
    hint: "failed transactions, per block",
    stems: ["fail", "error"],
    read: (row) => (has(row, HAS_BLOCK) ? row[16] : null),
  },
  {
    source: "slots",
    key: "compute",
    label: "Compute",
    unit: "million CU per block",
    rate: false,
    hint: "CU: compute units used per block; how full blocks are",
    stems: ["comput", "cus?\\b"],
    read: (row) => (has(row, HAS_BLOCK) ? row[4] / 1e6 : null),
  },
  {
    source: "slots",
    key: "fees",
    label: "Fees",
    unit: "SOL per block",
    rate: false,
    hint: "all transaction fees, per block",
    stems: ["fee"],
    read: (row) => (has(row, HAS_BLOCK) ? row[5] / LAMPORTS_PER_SOL : null),
  },
  {
    source: "slots",
    key: "priority_fees",
    label: "Priority fees",
    unit: "SOL per block",
    rate: false,
    hint: "priority fees only, per block",
    stems: ["priorit"],
    read: (row) => (has(row, HAS_BLOCK) ? row[6] / LAMPORTS_PER_SOL : null),
  },
  {
    source: "slots",
    key: "tips",
    label: "Tips",
    unit: "SOL per block",
    rate: false,
    hint: "tips paid to the leader, per block",
    stems: ["tip"],
    read: (row) => (has(row, HAS_BLOCK | HAS_TIPS) ? row[7] / LAMPORTS_PER_SOL : null),
  },
  {
    source: "slots",
    key: "replay",
    label: "Replay time",
    unit: "ms per block",
    rate: false,
    hint: "time taken to replay a block",
    stems: ["replay"],
    read: (row) => (has(row, HAS_REPLAY) ? row[9] / 1000 : null),
  },
  {
    source: "slots",
    key: "fill",
    label: "Time to full",
    unit: "ms per block",
    rate: false,
    hint: "time for a block to arrive, from its first shred to the full block",
    stems: ["fill", "full", "arriv"],
    read: (row) => (has(row, HAS_SHREDS) ? row[12] : null),
  },
];

const GIB = 2 ** 30;

/** Named as `host.range` names them. */
export const HOST_SERIES: readonly HostSeries[] = [
  {
    source: "host",
    key: "cpu",
    label: "CPU use",
    unit: "percent",
    hint: "share of the machine's processors in use, percent; CPU usage",
    stems: ["cpu", "processor"],
    scale: 100,
  },
  {
    source: "host",
    key: "load",
    label: "Load",
    unit: "one-minute average",
    hint: "the machine's load average",
    stems: ["load average", "system load", "loadavg", "load\\b"],
    scale: 1,
  },
  {
    source: "host",
    key: "memory",
    label: "Memory in use",
    unit: "GiB",
    hint: "memory in use on the whole machine, all processes together; RAM",
    stems: ["memor", "ram\\b"],
    scale: 1 / GIB,
  },
  {
    source: "host",
    key: "validator_memory",
    label: "Validator memory",
    unit: "GiB",
    hint: "memory used by the validator process alone; resident, RSS",
    stems: ["validator(?:'s)? memor", "resident", "rss\\b", "process memor"],
    scale: 1 / GIB,
  },
  {
    source: "host",
    key: "net_in",
    label: "Network in",
    unit: "MB per second",
    hint: "network traffic the machine receives; ingress, inbound, download",
    stems: ["(?:network in\\b(?! (?:the|a|this|that)\\b)|ingress|inbound)", "download"],
    scale: 1e-6,
  },
  {
    source: "host",
    key: "net_out",
    label: "Network out",
    unit: "MB per second",
    hint: "network traffic the machine sends; egress, outbound, upload",
    stems: ["(?:network out\\b|egress|outbound)", "upload"],
    scale: 1e-6,
  },
  {
    source: "host",
    key: "disk_used",
    label: "Disk used",
    unit: "percent of the fullest filesystem",
    hint: "how full the disks are, the fullest one; disk space, storage",
    stems: ["disk (?:us|space|full)", "full (?:are|is) the disks?", "disks? (?:are|is) full", "storage", "filesystem"],
    scale: 100,
  },
  {
    source: "host",
    key: "disk_busy",
    label: "Disk busy",
    unit: "percent of the busiest disk",
    hint: "how busy the disks are, the busiest one; disk I/O, utilisation",
    stems: ["disk (?:bus|i/?o|util|activ|load)", "busy (?:are|is) the disks?", "iops\\b"],
    scale: 100,
  },
  {
    source: "host",
    key: "udp_drops",
    label: "UDP drops",
    unit: "per second",
    hint: "packets the kernel dropped on the validator's UDP ports",
    stems: ["udp", "dropp?(?:ed|ing)? packets", "packet drops?", "kernel drops?"],
    scale: 1,
  },
];

export const SERIES: readonly Series[] = [...SLOT_SERIES, ...HOST_SERIES];

/** The model's answer for a request no series fits. */
export const NONE = "none";

export const MIN_WINDOW_MILLIS = 5 * 60_000;
export const MAX_WINDOW_MILLIS = 6 * 3_600_000;
const DEFAULT_WINDOW_MILLIS = 3_600_000;

export const WINDOWS: readonly { label: string; millis: number }[] = [
  { label: "15 min", millis: 15 * 60_000 },
  { label: "1 hour", millis: 3_600_000 },
  { label: "6 hours", millis: MAX_WINDOW_MILLIS },
];

export interface AskedWindow {
  millis: number;
  /** False when the request named no window and the default stands in. */
  given: boolean;
  /** The request named a window outside what the page draws. */
  clamped: boolean;
}

const NUMBER_WORDS: Record<string, number> = {
  one: 1,
  two: 2,
  three: 3,
  four: 4,
  five: 5,
  six: 6,
  seven: 7,
  eight: 8,
  nine: 9,
  ten: 10,
  eleven: 11,
  twelve: 12,
  fifteen: 15,
  twenty: 20,
  thirty: 30,
  forty: 40,
  fifty: 50,
  sixty: 60,
  ninety: 90,
};

/** Each unit's words, its one-letter form, and its length. */
const UNITS: readonly [words: string, letter: string, millis: number][] = [
  ["seconds?|secs?", "s", 1000],
  ["minutes?|mins?", "m", 60_000],
  ["hours?|hrs?", "h", 3_600_000],
  ["days?", "d", 86_400_000],
  ["weeks?|wks?", "w", 7 * 86_400_000],
];

const WORDS = UNITS.map(([words]) => words).join("|");
const LETTERS = UNITS.map(([, letter]) => letter).join("");
/** Not the tail of a longer number or word, so "1.5 hours" is not five hours. */
const START = "(?:^|[^\\w.,])";
/** A one-letter unit counts only hard against its digits: "5m", never "5 m" or the "d" of "and". */
const DIGITS_AND_UNIT = new RegExp(`${START}(\\d+(?:\\.\\d+)?)(?:[\\s-]*(${WORDS})|([${LETTERS}]))\\b`);
const NUMBER_WORD = Object.keys(NUMBER_WORDS).join("|");
/** Tens may lead a smaller word, as in "forty-five". */
const WORD_AND_UNIT = new RegExp(`${START}(?:(twenty|thirty|forty|fifty)[\\s-]+)?(${NUMBER_WORD})[\\s-]+(${WORDS})\\b`);
const AND_A_HALF = new RegExp(`${START}(\\d+|an?|${NUMBER_WORD}) and a half hours?\\b`);
/** "a minute" alone is a rate, as in "transactions a minute"; after one of these it is a window. */
/** Seconds are left out: "in the second half" is not a window. */
const ONE_UNIT = new RegExp(
  `\\b(?:past|last|previous|this|for|over|in|within)\\s+(?:an?\\s+|the\\s+)?(${UNITS.slice(1)
    .map(([words]) => words)
    .join("|")})\\b`,
);

function unitMillis(unit: string): number {
  return UNITS.find(([words, letter]) => unit === letter || new RegExp(`^(?:${words})$`).test(unit))?.[2] ?? 0;
}

function named(text: string): number | null {
  if (/\bhour and a half\b/.test(text)) return 90 * 60_000;
  const halved = AND_A_HALF.exec(text);
  if (halved) return ((NUMBER_WORDS[halved[1]] ?? (Number(halved[1]) || 1)) + 0.5) * 3_600_000;
  if (/\bhalf (?:an? )?hour\b/.test(text)) return 30 * 60_000;
  if (/\bquarter (?:of an? )?hour\b/.test(text)) return 15 * 60_000;
  const digits = DIGITS_AND_UNIT.exec(text);
  if (digits) return Number(digits[1]) * unitMillis(digits[2] ?? digits[3]);
  const word = WORD_AND_UNIT.exec(text);
  if (word) {
    const ones = NUMBER_WORDS[word[2]];
    const tens = word[1] !== undefined && ones < 10 ? NUMBER_WORDS[word[1]] : 0;
    return (tens + ones) * unitMillis(word[3]);
  }
  const one = ONE_UNIT.exec(text);
  return one ? unitMillis(one[1]) : null;
}

/** The window a request names, held to what the page draws. */
export function windowOf(typed: string): AskedWindow {
  const millis = named(typed.toLowerCase());
  if (millis === null || !(millis > 0)) return { millis: DEFAULT_WINDOW_MILLIS, given: false, clamped: false };
  const held = Math.min(Math.max(millis, MIN_WINDOW_MILLIS), MAX_WINDOW_MILLIS);
  return { millis: held, given: true, clamped: held !== millis };
}

const WHY =
  /\bwhy\b|\bexplain\b|\bhow come\b|\bwhat (?:caused|factors|affected|went wrong)\b|\b(?:cause|reason)s? (?:of|for|behind)\b/;

/** A request for a cause, which this page does not answer and so never sends to the model. */
export function asksWhy(typed: string): boolean {
  return WHY.test(typed.toLowerCase());
}

/** Whether a request holds a word its chosen series usually goes by. */
export function namesSeries(typed: string, series: Series): boolean {
  return new RegExp(`\\b(?:${series.stems.join("|")})`, "i").test(typed);
}

/** The most charts one request draws. */
export const MAX_SERIES = 3;

/** Enums only: an open string field lets the model run on. The browser refuses `uniqueItems`, so
 *  a repeated key is dropped on reading the reply. */
export function seriesSchema(series: readonly Series[]): object {
  return {
    type: "object",
    properties: {
      series: {
        type: "array",
        items: { type: "string", enum: [...series.map((entry) => entry.key), NONE] },
        minItems: 1,
        maxItems: MAX_SERIES,
      },
    },
    required: ["series"],
    additionalProperties: false,
  };
}

export function systemPrompt(series: readonly Series[]): string {
  return [
    "A request asks to see charts of things recorded about a Solana validator and the machine it runs on. Answer with the key of each series it asks for, in the order asked. Most requests ask for one; never add a series the request does not ask for.",
    `Answer ${NONE} for anything asked for that is not listed, such as a temperature, a price, a latency or one protocol's traffic. Do not pick the nearest.`,
    "Series:",
    ...series.map((entry) => `${entry.key}: ${entry.hint}`),
  ].join("\n");
}

export interface Reply {
  /** In the order the model gave them, each once, `MAX_SERIES` at most; empty when nothing matched. */
  series: Series[];
  /** The model marked part of the request as not recorded, beside a series it did find. */
  unmatched: boolean;
}

/** What the model named, or null for an answer outside the schema. */
export function parseReply(text: string, series: readonly Series[]): Reply | null {
  let reply: unknown;
  try {
    reply = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof reply !== "object" || reply === null || !("series" in reply) || !Array.isArray(reply.series)) return null;
  const found: Series[] = [];
  let none = false;
  for (const key of reply.series as unknown[]) {
    if (key === NONE) {
      none = true;
      continue;
    }
    const named = series.find((entry) => entry.key === key);
    if (!named) return null;
    if (!found.includes(named)) found.push(named);
  }
  return { series: found.slice(0, MAX_SERIES), unmatched: none && found.length > 0 };
}

/** A request for more than one thing joins them with one of these. */
const JOINER = /[,&]|\b(?:and|plus|vs|versus|against|separate|both)\b/;

/** The series a request asked for and did not get, in the order asked. Only a request that joins
 *  several things has any, and a series counts by its first stem alone, outside a "per" unit and
 *  outside the words a picked series' own label and hint hold. */
export function leftOut(typed: string, picked: readonly Series[], all: readonly Series[]): Series[] {
  const text = typed.toLowerCase().replace(/\bper\s+\w+/g, " ");
  if (!JOINER.test(text)) return [];
  return all
    .map((entry) => ({ entry, at: text.search(new RegExp(`\\b${entry.stems[0]}`)) }))
    .filter(
      ({ entry, at }) =>
        at !== -1 &&
        !picked.includes(entry) &&
        !picked.some((drawn) => namesSeries(`${drawn.label} ${drawn.hint}`, entry)),
    )
    .sort((first, second) => first.at - second.at)
    .map(({ entry }) => entry);
}

/** The line under a request part of which was not drawn, or null when all of it was. */
export function notDrawnNote(left: readonly Series[], unmatched: boolean): string | null {
  const parts: string[] = [];
  if (left.length > 0) {
    const names = left.map((entry) => inSentence(entry.label)).join(", ");
    parts.push(`Not drawn: ${names}. Ask for ${left.length === 1 ? "it on its own" : "them on their own"}.`);
  }
  if (unmatched) parts.push("Part of this request matches nothing recorded.");
  return parts.length > 0 ? parts.join(" ") : null;
}

/** A label as it reads inside a sentence: lower case, but for a leading acronym. */
function inSentence(label: string): string {
  return /^[A-Z][a-z]/.test(label) ? label.charAt(0).toLowerCase() + label.slice(1) : label;
}

/** What a refusal lists as recorded. */
export function recordedList(series: readonly Series[]): string {
  return series.map((entry) => inSentence(entry.label)).join(", ");
}

export function windowLabel(millis: number): string {
  const hours = millis / 3_600_000;
  if (hours === 1) return "past hour";
  if (Number.isInteger(hours)) return `past ${hours} hours`;
  return `past ${Math.round(millis / 60_000)} minutes`;
}

/** A window that is none of the page's own, as its toggle names it. */
export function windowShort(millis: number): string {
  const hours = millis / 3_600_000;
  return Number.isInteger(hours) ? `${hours} hour${hours === 1 ? "" : "s"}` : `${Math.round(millis / 60_000)} min`;
}

const PAGE_SLOTS = 4096;
export const MAX_PAGES = 30;
/** A restart leaves a run of slots with no row; past this many pages of them the history has ended. */
export const MAX_EMPTY_PAGES = 3;

export interface WindowRows {
  rows: [time: number, row: WireRow, slot: number][];
  /** False when the history ends inside the window. */
  reached: boolean;
}

/** The rows whose blocks began at or after `from`, walking back from the tip a page at a time. */
export async function readWindow(
  range: (first: number, count: number) => Promise<SlotRange>,
  tip: number,
  from: number,
): Promise<WindowRows> {
  const rows: [number, WireRow, number][] = [];
  let end = tip + 1;
  let empty = 0;
  for (let page = 0; page < MAX_PAGES && end > 0 && empty < MAX_EMPTY_PAGES; page += 1) {
    const first = Math.max(0, end - PAGE_SLOTS);
    const reply = await range(first, end - first);
    let clocked = 0;
    for (let index = reply.rows.length - 1; index >= 0; index -= 1) {
      const row = reply.rows[index];
      if (!row || !has(row, HAS_CLOCK)) continue;
      clocked += 1;
      if (row[8] < from) return { rows, reached: true };
      rows.push([row[8], row, reply.first_slot + index]);
    }
    empty = clocked === 0 ? empty + 1 : 0;
    end = first;
  }
  return { rows, reached: false };
}

/** One block's reading, with where it was. */
export interface Extreme {
  value: number;
  time: number;
  slot: number | null;
}

/** What the window's blocks add up to, read from each block rather than from the chart's spans. */
export interface BlockFigures {
  total: number;
  count: number;
  /** Per block. */
  mean: number;
  low: Extreme;
  high: Extreme;
  /** The value the blocks were counted against, if any, and how many were over and under it, or
   *  at least and at most it where `inclusive`. */
  threshold?: number;
  inclusive: boolean;
  over: number;
  under: number;
}

export interface Bins {
  from: number;
  to: number;
  /** Each span's reading, or null where it holds none; the line drawn. */
  values: (number | null)[];
  /** Of the line: what the axis is scaled to. */
  min: number;
  max: number;
  mean: number;
  latest: number;
  /** The length of one span. */
  binMillis: number;
  /** When the oldest reading in the window was taken. */
  first: number;
  /** The highest single reading, where the values drawn are means of several. */
  peak?: number;
  /** Each step's highest single reading, where the server keeps one. */
  peaks?: (number | null)[];
  /** For per-slot series, figures from the blocks themselves. */
  blocks?: BlockFigures;
}

export const BIN_COUNT = 120;

/** The readings in equal spans of time, or null when the window holds none. The same pass adds up
 *  the blocks in the window and counts those over and under `threshold`. */
export function bucket(
  rows: readonly (readonly [number, WireRow, number?])[],
  series: SlotSeries,
  from: number,
  to: number,
  count = BIN_COUNT,
  threshold?: number,
  inclusive = false,
): Bins | null {
  const sums = new Array<number>(count).fill(0);
  const seen = new Array<number>(count).fill(0);
  const width = (to - from) / count;
  let total = 0;
  let readings = 0;
  let over = 0;
  let under = 0;
  let first = Infinity;
  let low: Extreme | null = null;
  let high: Extreme | null = null;
  for (const [time, row, slot] of rows) {
    const value = series.read(row);
    if (value === null) continue;
    const index = Math.min(count - 1, Math.max(0, Math.floor((time - from) / width)));
    sums[index] += value;
    seen[index] += 1;
    if (time < from || time >= to) continue;
    total += value;
    readings += 1;
    first = Math.min(first, time);
    if (threshold !== undefined && (inclusive ? value >= threshold : value > threshold)) over += 1;
    if (threshold !== undefined && (inclusive ? value <= threshold : value < threshold)) under += 1;
    if (!low || value < low.value) low = { value, time, slot: slot ?? null };
    if (!high || value > high.value) high = { value, time, slot: slot ?? null };
  }
  // The newest span is still filling, so a rate there reads low.
  const values = sums.map((sum, index) =>
    seen[index] === 0 || index === count - 1 ? null : series.rate ? sum / (width / 1000) : sum / seen[index],
  );
  const real = values.filter((value): value is number => value !== null);
  if (real.length === 0) return null;
  return {
    from,
    to,
    values,
    min: Math.min(...real),
    max: Math.max(...real),
    mean: real.reduce((sum, value) => sum + value, 0) / real.length,
    latest: real[real.length - 1],
    binMillis: width,
    first: Number.isFinite(first) ? first : from,
    blocks:
      low && high ? { total, count: readings, mean: total / readings, low, high, threshold, inclusive, over, under } : undefined,
  };
}

/** What `host.range` answers. */
export interface HostRange {
  from: number;
  to: number;
  step_ms: number;
  /** When the readings held begin, or null with none held. */
  held_from: number | null;
  series: { key: string; mean: (number | null)[]; max: (number | null)[] }[];
}

/** The span of one bin of host readings on the server, which a window is lined up to. */
export const HOST_STEP_MILLIS = 10_000;

/** One host series of a reply as a chart's readings, or null when the window holds none. The line
 *  and its figures are each step's mean; the peak is the highest single reading. */
export function hostBins(range: HostRange, series: HostSeries): Bins | null {
  const held = range.series.find((entry) => entry.key === series.key);
  if (!held) return null;
  const values = held.mean.map((value) => (value === null ? null : value * series.scale));
  const real = values.filter((value): value is number => value !== null);
  if (real.length === 0) return null;
  const peaks = held.max.map((value) => (value === null ? null : value * series.scale));
  const max = Math.max(...real);
  const firstHeld = values.findIndex((value) => value !== null);
  return {
    from: range.from,
    to: range.to,
    values,
    min: Math.min(...real),
    max,
    mean: real.reduce((sum, value) => sum + value, 0) / real.length,
    latest: real[real.length - 1],
    binMillis: range.step_ms,
    first: range.from + firstHeld * range.step_ms,
    peak: Math.max(max, ...peaks.filter((value): value is number => value !== null)),
    peaks,
  };
}

export function figure(value: number): string {
  const size = Math.abs(value);
  const digits = size >= 100 ? 0 : size >= 10 ? 1 : size >= 1 ? 2 : 3;
  return value.toLocaleString("en-US", { maximumFractionDigits: digits });
}

/** The next round figure at or above the highest reading. */
export function ceiling(max: number): number {
  if (max <= 0) return 1;
  const step = 10 ** Math.floor(Math.log10(max));
  const nice = [1, 2, 2.5, 5, 10].find((mult) => mult * step >= max) ?? 10;
  return nice * step;
}

/** A chart's line and the area under it in a `width` by `height` box, each broken where a span has
 *  no reading. A reading alone between two gaps is a line to itself, which a round cap shows. */
export function paths(
  values: readonly (number | null)[],
  top: number,
  width: number,
  height: number,
): { line: string; area: string } {
  const step = values.length > 1 ? width / (values.length - 1) : 0;
  const x = (index: number): string => (index * step).toFixed(1);
  let line = "";
  let area = "";
  let last: number | null = null;
  let run = 0;
  const close = (): void => {
    if (last === null) return;
    if (run === 1) line += `L${x(last)},${line.slice(line.lastIndexOf(",") + 1)}`;
    area += `L${x(last)},${height}Z`;
  };
  values.forEach((value, index) => {
    if (value === null) {
      close();
      last = null;
      run = 0;
      return;
    }
    const point = `${x(index)},${(height - (value / top) * height).toFixed(1)}`;
    line += `${last === null ? "M" : "L"}${point}`;
    area += last === null ? `M${x(index)},${height}L${point}` : `L${point}`;
    last = index;
    run += 1;
  });
  close();
  return { line, area };
}
