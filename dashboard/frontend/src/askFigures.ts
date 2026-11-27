/** Figures the Ask page works out beside a chart: a total, a mean, a peak, a share, or when a series
 *  was over or under a value. The model only names the operation; the value is read here. */

import { figure, SERIES, type Bins, type Series } from "./ask";

export const OPERATIONS = ["chart", "total", "mean", "highest", "lowest", "share", "when_above", "when_below"] as const;

export type Operation = (typeof OPERATIONS)[number];

/** As measured on the built-in model; a test holds it to that text. */
export const OPERATION_PROMPT = [
  "A request asks about things recorded about a Solana validator and the machine it runs on. Answer the operation it asks for.",
  "chart: draw them over time. Most requests want this, including a plain name or a question about how something has been.",
  "total: a sum over the window, how much or how many in all.",
  "mean: an average or a typical value.",
  "highest or lowest: a peak or maximum, or a minimum.",
  "share: what share, fraction or percentage one thing is of another.",
  "when_above or when_below: when something went over or under a value.",
].join("\n");

export function operationSchema(): object {
  return {
    type: "object",
    properties: { operation: { type: "string", enum: [...OPERATIONS] } },
    required: ["operation"],
    additionalProperties: false,
  };
}

/** The operation the model named, or null for an answer outside the schema. */
export function parseOperation(text: string): Operation | null {
  let reply: unknown;
  try {
    reply = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof reply !== "object" || reply === null || !("operation" in reply)) return null;
  const named = reply.operation;
  return OPERATIONS.find((operation) => operation === named) ?? null;
}

const byKey = (key: string): Series | undefined => SERIES.find((entry) => entry.key === key);

/** Each part and the whole it is a share of. */
export const SHARE_OF: Readonly<Record<string, string>> = {
  failed: "transactions",
  repaired: "shreds",
  recovered: "shreds",
  priority_fees: "fees",
};

export interface SharePair {
  part: Series;
  whole: Series;
  /** The whole was not picked and comes from the table. */
  byRule: boolean;
}

/** The part and whole of a share among the picked series, the second picked being the whole. */
export function sharePair(picked: readonly Series[]): SharePair | null {
  const [one, two] = picked;
  if (!one) return null;
  if (two) {
    if (SHARE_OF[one.key] === two.key) return { part: one, whole: two, byRule: false };
    if (SHARE_OF[two.key] === one.key) return { part: two, whole: one, byRule: false };
    return null;
  }
  const whole = SHARE_OF[one.key] ? byKey(SHARE_OF[one.key]) : undefined;
  return whole ? { part: one, whole, byRule: true } : null;
}

export const NO_SHARE = "A share is worked out only for failed transactions, repaired or recovered shreds and priority fees.";

/** Counted series, whose total is a sum; the rest are levels or times, whose total means nothing. */
const TOTALS: Readonly<Record<string, string>> = {
  shreds: "shreds",
  repaired: "shreds",
  recovered: "shreds",
  transactions: "transactions",
  votes: "votes",
  failed: "transactions",
  compute: "billion CU",
  fees: "SOL",
  priority_fees: "SOL",
  tips: "SOL",
  udp_drops: "datagrams",
  net_in: "GB",
  net_out: "GB",
};

/** Null where the operation fits the series, otherwise why it does not. */
export function unfit(operation: Operation, series: Series): string | null {
  if (operation === "total" && !(series.key in TOTALS)) return `A total of ${inSentence(series.label)} is not meaningful.`;
  return null;
}

function inSentence(label: string): string {
  return /^[A-Z][a-z]/.test(label) ? label.charAt(0).toLowerCase() + label.slice(1) : label;
}

/** The unit a series is read in, as a figure line names it. */
const UNIT: Readonly<Record<string, string>> = {
  shreds: "per second",
  transactions: "per second",
  votes: "per second",
  repaired: "per block",
  recovered: "per block",
  failed: "per block",
  compute: "million CU",
  fees: "SOL",
  priority_fees: "SOL",
  tips: "SOL",
  replay: "ms",
  fill: "ms",
  cpu: "percent",
  load: "",
  memory: "GiB",
  validator_memory: "GiB",
  net_in: "MB per second",
  net_out: "MB per second",
  disk_used: "percent",
  disk_busy: "percent",
  udp_drops: "per second",
};

const withUnit = (value: number, key: string): string => `${figure(value)}${UNIT[key] ? ` ${UNIT[key]}` : ""}`;

/** The units a value may come in for each kind of series, each with what turns it into the series' own. */
type Units = readonly [pattern: RegExp, factor: (value: number) => number][];

const SAME = (value: number): number => value;

const TIME_UNITS: Units = [
  [/^(?:ms|millis(?:econds?)?|milliseconds?)$/, SAME],
  [/^(?:s|secs?|seconds?)$/, (value) => value * 1000],
];
const PERCENT_UNITS: Units = [[/^(?:%|percent|pct)$/, SAME]];
const MEMORY_UNITS: Units = [
  [/^(?:gib|gigs?)$/, SAME],
  [/^(?:gb|gigabytes?)$/, (value) => (value * 1e9) / 2 ** 30],
  [/^(?:mib)$/, (value) => value / 1024],
  [/^(?:mb|megabytes?)$/, (value) => (value * 1e6) / 2 ** 30],
  [/^(?:tib)$/, (value) => value * 1024],
  [/^(?:tb|terabytes?)$/, (value) => (value * 1e12) / 2 ** 30],
];
const NETWORK_UNITS: Units = [
  [/^(?:mbps|mbit\/?s?|megabits?(?: per second)?)$/, (value) => value / 8],
  [/^(?:gbps|gbit\/?s?|gigabits?(?: per second)?)$/, (value) => value * 125],
  [/^(?:mb\/s|mb|megabytes?(?: per second)?|mb per second)$/, SAME],
  [/^(?:gb\/s|gb|gigabytes?(?: per second)?|gb per second)$/, (value) => value * 1000],
  [/^(?:kb\/s|kb|kilobytes?(?: per second)?)$/, (value) => value / 1000],
];
const RATE_UNITS: Units = [
  [/^(?:per second|a second|\/s|tps|pps|each second)$/, SAME],
  [/^(?:per minute|a minute|\/min|each minute)$/, (value) => value / 60],
];
const SOL_UNITS: Units = [
  [/^(?:sol)$/, SAME],
  [/^(?:lamports?)$/, (value) => value / 1e9],
];
const COMPUTE_UNITS: Units = [[/^(?:m|million|million cu|mcu)$/, SAME]];

function unitsOf(key: string): Units {
  switch (key) {
    case "replay":
    case "fill":
      return TIME_UNITS;
    case "cpu":
    case "disk_used":
    case "disk_busy":
      return PERCENT_UNITS;
    case "memory":
    case "validator_memory":
      return MEMORY_UNITS;
    case "net_in":
    case "net_out":
      return NETWORK_UNITS;
    case "shreds":
    case "transactions":
    case "votes":
    case "udp_drops":
      return RATE_UNITS;
    case "fees":
    case "priority_fees":
    case "tips":
      return SOL_UNITS;
    case "compute":
      return COMPUTE_UNITS;
    default:
      return [];
  }
}

/** Every unit any series is read in, to tell a value in another series' unit from a bare one. */
const ANY_UNIT: Units = [
  ...TIME_UNITS,
  ...PERCENT_UNITS,
  ...MEMORY_UNITS,
  ...NETWORK_UNITS,
  ...RATE_UNITS,
  ...SOL_UNITS,
  ...COMPUTE_UNITS,
];

const ABOVE = "over|above|more than|greater than|higher than|exceed(?:s|ed|ing)?|at least|beyond|went over|went above";
const BELOW = "under|below|less than|lower than|fewer than|at most|dipped (?:below|under)|fell (?:below|under)|dropped (?:below|under)";
/** A window, never a value: "over the past 2 hours", "under 30 minutes". */
const WINDOW_UNIT = /^(?:m|mins?|minutes?|h|hrs?|hours?|d|days?|w|wks?|weeks?)$/;
const COMPARISON = new RegExp(
  `\\b(${ABOVE}|${BELOW})\\s+(\\d[\\d,]*(?:\\.\\d+)?)\\s*(%|(?:per|a|each)\\s+(?:second|minute)|[a-z/]+(?:\\s+(?:per|a)\\s+(?:second|minute))?)?`,
  "g",
);
/** Comparing words with no number after them, so the answer can ask for one. */
const COMPARING = new RegExp(`\\b(?:${ABOVE}|${BELOW})\\b`);

export interface Threshold {
  above: boolean;
  /** In the series' own unit. */
  value: number;
  /** The words in the request it was read from, to take out before the window is read. */
  matched: string;
}

export type ThresholdRead = Threshold | { wrong: string } | null;

/** The value a request compares a series against, in that series' unit; `wrong` for a value in a
 *  unit the series is not read in, null for none. */
export function thresholdOf(typed: string, series: Series): ThresholdRead {
  for (const found of typed.toLowerCase().matchAll(COMPARISON)) {
    const read = comparison(found, series);
    if (read) return read;
  }
  return null;
}

/** One comparing phrase read for `series`, or null where it names a window instead. */
function comparison(found: RegExpMatchArray, series: Series): ThresholdRead {
  const [matched, comparator, number, rawUnit] = found;
  let unit = (rawUnit ?? "").trim();
  const millions = series.key === "compute" && unit === "m";
  if (!millions && WINDOW_UNIT.test(unit)) return null;
  // A second is a window for every series but those read in time.
  const timed = series.key === "replay" || series.key === "fill";
  if (!timed && /^(?:s|secs?|seconds?)$/.test(unit)) return null;
  const value = Number(number.replace(/,/g, ""));
  if (!Number.isFinite(value)) return null;
  const above = new RegExp(`^(?:${ABOVE})$`).test(comparator);
  let factor = SAME;
  if (unit) {
    const fits = unitsOf(series.key).find(([pattern]) => pattern.test(unit));
    if (fits) factor = fits[1];
    else if (ANY_UNIT.some(([pattern]) => pattern.test(unit))) {
      return { wrong: `${number} ${unit} is not a unit ${inSentence(series.label)} is read in.` };
    } else {
      // A word after the number that is no unit, as in "over 50 in the last hour".
      unit = "";
    }
  }
  let read = factor(value);
  // A bare figure for compute over a hundred thousand is in units, not millions.
  if (series.key === "compute" && !unit && value > 100_000) read = value / 1e6;
  const words = unit ? matched : matched.slice(0, matched.lastIndexOf(number) + number.length);
  return { above, value: read, matched: words };
}

/** The request with its comparison taken out, so "under 5 s" is not read as a window. */
export function withoutThreshold(typed: string, series: Series): string {
  const read = thresholdOf(typed, series);
  if (!read || "wrong" in read) return typed;
  const at = typed.toLowerCase().indexOf(read.matched);
  return at < 0 ? typed : `${typed.slice(0, at)} ${typed.slice(at + read.matched.length)}`;
}

export interface Span {
  start: number;
  end: number;
}

/** Runs of spans where `test` holds, each from its first span's start to its last span's end. A
 *  span with no reading ends a run. */
export function spansOf(
  values: readonly (number | null)[],
  from: number,
  binMillis: number,
  test: (value: number) => boolean,
): Span[] {
  const spans: Span[] = [];
  let open: number | null = null;
  values.forEach((value, index) => {
    const holds = value !== null && test(value);
    if (holds && open === null) open = index;
    if (!holds && open !== null) {
      spans.push({ start: from + open * binMillis, end: from + index * binMillis });
      open = null;
    }
  });
  if (open !== null) spans.push({ start: from + open * binMillis, end: from + values.length * binMillis });
  return spans;
}

/** A length of time as a figure line gives it. */
export function lasting(millis: number): string {
  const seconds = Math.round(millis / 1000);
  if (seconds < 60) return `${seconds} s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours} h ${rest} min` : `${hours} h`;
}

/** What a panel was read as: the operation and series, then the unit. */
export function readAs(operation: Operation | null, series: Series, threshold: ThresholdRead): { head: string; unit: string } {
  const unit = series.key in UNIT ? (UNIT[series.key] ?? "") : "";
  switch (operation) {
    case "total":
      return unfit(operation, series) ? { head: series.label, unit } : { head: `Total of ${series.label}`, unit: TOTALS[series.key] ?? "" };
    case "mean":
      return { head: `Mean of ${series.label}`, unit };
    case "highest":
      return { head: `Highest ${inSentence(series.label)}`, unit };
    case "lowest":
      return { head: `Lowest ${inSentence(series.label)}`, unit };
    case "when_above":
    case "when_below":
      return threshold && !("wrong" in threshold)
        ? { head: `When ${series.label} was ${threshold.above ? "over" : "under"} ${withUnit(threshold.value, series.key)}`, unit: "" }
        : { head: series.label, unit };
    default:
      return { head: series.label, unit };
  }
}

export interface Mark {
  threshold: number;
  spans: readonly Span[];
}

export interface Figure {
  label: string;
  value: string;
  detail: string;
  /** The window holds less than was asked, or the figure is near. */
  warn: boolean;
  mark?: Mark;
}

export type Answered = Figure | { unfit: string } | null;

export interface FigureContext {
  /** The window asked for, as "past hour". */
  window: string;
  windowMillis: number;
  threshold: ThresholdRead;
  /** The request held a comparing word. */
  comparing: boolean;
  clock: (millis: number) => string;
}

/** Where the window's readings begin, when well after it opens: a figure over less than was asked. */
function shortfall(bins: Bins, context: FigureContext): { label: string; note: string } | null {
  if (bins.first <= bins.from + bins.binMillis) return null;
  return {
    label: `since ${context.clock(bins.first)}`,
    note: `the history holds ${lasting(bins.to - bins.first)} of the ${context.window}`,
  };
}

/** A host series' total over its steps, the last of which may be shorter; `near` where a step with
 *  readings borders one without, which is then counted whole. */
function hostTotal(bins: Bins, key: string): { total: number; near: boolean } {
  let total = 0;
  let near = false;
  bins.values.forEach((value, index) => {
    if (value === null) return;
    const start = bins.from + index * bins.binMillis;
    const seconds = Math.min(bins.binMillis, bins.to - start) / 1000;
    total += value * seconds;
    if (bins.values[index - 1] === null || bins.values[index + 1] === null) near = true;
  });
  // Network is read in megabytes a second and totalled in gigabytes.
  return { total: key === "net_in" || key === "net_out" ? total / 1000 : total, near };
}

/** A per-block series' unit, read per block. */
const perBlock = (key: string): string => (UNIT[key] === "per block" ? "per block" : `${UNIT[key]} per block`);

/** The figure an operation gives for one series. */
export function answerFigure(operation: Operation | null, series: Series, bins: Bins | null, context: FigureContext): Answered {
  if (!operation || operation === "chart" || operation === "share" || !bins) return null;
  const why = unfit(operation, series);
  if (why) return { unfit: why };
  const short = shortfall(bins, context);
  const over = short ? short.label : context.window;
  const name = inSentence(series.label);
  const notes = (detail: string): string => [detail, short?.note].filter(Boolean).join(" · ");
  const rate = series.source === "slots" && series.rate;
  const blocks = series.source === "slots" ? bins.blocks : undefined;
  const span = lasting(bins.binMillis);
  const at = (index: number): number => bins.from + index * bins.binMillis;
  switch (operation) {
    case "total": {
      if (series.source === "host") {
        const { total, near } = hostTotal(bins, series.key);
        return {
          label: `Total ${name}, ${over}`,
          value: `${near ? "about " : ""}${figure(total)} ${TOTALS[series.key]}`,
          detail: notes(`from readings in ${span} steps`),
          warn: Boolean(short),
        };
      }
      if (!blocks) return null;
      const total = series.key === "compute" ? blocks.total / 1000 : blocks.total;
      const whose = series.key === "fees" || series.key === "priority_fees" || series.key === "tips" ? " from every leader" : "";
      return {
        label: `Total ${name}, ${over}`,
        value: `${figure(total)} ${TOTALS[series.key]}`,
        detail: notes(`${figure(blocks.count)} blocks${whose}`),
        warn: Boolean(short),
      };
    }
    case "mean": {
      if (blocks && !rate) {
        return {
          label: `Mean ${name}, ${over}`,
          value: `${figure(blocks.mean)} ${perBlock(series.key)}`,
          detail: notes(`${figure(blocks.count)} blocks`),
          warn: Boolean(short),
        };
      }
      return {
        label: `Mean ${name}, ${over}`,
        value: `${figure(bins.mean)}${UNIT[series.key] ? ` ${UNIT[series.key]}` : ""}`,
        detail: notes(series.source === "host" ? `from readings in ${span} steps` : `over the ${span} spans drawn`),
        warn: Boolean(short),
      };
    }
    case "highest":
    case "lowest": {
      const highest = operation === "highest";
      const word = highest ? "Highest" : "Lowest";
      if (blocks && !rate) {
        const block = highest ? blocks.high : blocks.low;
        const where = block.slot === null ? `at ${context.clock(block.time)}` : `slot ${figure(block.slot)} at ${context.clock(block.time)}`;
        return { label: `${word} ${name}, ${over}`, value: withUnit(block.value, series.key), detail: notes(where), warn: Boolean(short) };
      }
      if (series.source === "host" && highest && bins.peaks) {
        let index = -1;
        bins.peaks.forEach((value, place) => {
          if (value !== null && (index < 0 || value > (bins.peaks?.[index] ?? -Infinity))) index = place;
        });
        if (index >= 0) {
          return {
            label: `${word} ${name}, ${over}`,
            value: withUnit(bins.peaks[index] ?? 0, series.key),
            detail: notes(`one-second reading, in the ${span} from ${context.clock(at(index))}`),
            warn: Boolean(short),
          };
        }
      }
      let index = -1;
      bins.values.forEach((value, place) => {
        if (value === null) return;
        const held = index < 0 ? null : bins.values[index];
        if (held === null || held === undefined || (highest ? value > held : value < held)) index = place;
      });
      if (index < 0) return null;
      return {
        label: `${word} ${name}, ${over}`,
        value: withUnit(bins.values[index] ?? 0, series.key),
        detail: notes(`${span} average, at ${context.clock(at(index))}`),
        warn: Boolean(short),
      };
    }
    case "when_above":
    case "when_below": {
      const threshold = context.threshold;
      if (!threshold) {
        return context.comparing ? { unfit: 'No value to compare against; ask, for example, "over 20 ms".' } : null;
      }
      if ("wrong" in threshold) return { unfit: threshold.wrong };
      const above = threshold.above;
      const test = above ? (value: number) => value > threshold.value : (value: number) => value < threshold.value;
      const spans = spansOf(bins.values, bins.from, bins.binMillis, test);
      const side = above ? "over" : "under";
      const mark = { threshold: threshold.value, spans };
      const by = `by ${span} ${series.source === "host" ? "steps" : "averages"}`;
      const counted = blocks && !rate ? `${figure(above ? blocks.over : blocks.under)} of ${figure(blocks.count)} blocks were ${side}` : null;
      let peaked: string | null = null;
      if (series.source === "host" && above && bins.peaks) {
        const steps = bins.peaks.filter((value) => value !== null && value > threshold.value).length;
        peaked = `${figure(steps)} steps had a one-second reading ${side}`;
      }
      const value = withUnit(threshold.value, series.key);
      if (spans.length === 0) {
        const extreme = above ? bins.max : bins.min;
        return {
          label: `Never ${side} ${value} ${short ? short.label : `in the ${context.window}`}`,
          value: "",
          detail: notes([`${above ? "highest" : "lowest"} ${span} average ${figure(extreme)}`, counted, peaked].filter(Boolean).join(" · ")),
          warn: Boolean(short),
          mark,
        };
      }
      const lengths = spans.map((one) => one.end - one.start);
      const longest = spans[lengths.indexOf(Math.max(...lengths))];
      const all = lengths.reduce((sum, length) => sum + length, 0);
      return {
        label: `${side.charAt(0).toUpperCase()}${side.slice(1)} ${value} in ${spans.length} span${spans.length === 1 ? "" : "s"}, ${lasting(all)} in all`,
        value: "",
        detail: notes(
          [`longest ${context.clock(longest.start)}–${context.clock(longest.end)}`, by, counted, peaked].filter(Boolean).join(" · "),
        ),
        warn: Boolean(short),
        mark,
      };
    }
    default:
      return null;
  }
}

/** A share's figure: the part's total over the whole's, from the blocks of the window. */
export function shareFigure(pair: SharePair | null, part: Bins | null, whole: Bins | null, context: FigureContext): Answered {
  if (!pair) return { unfit: NO_SHARE };
  const partTotal = part?.blocks?.total;
  const wholeTotal = whole?.blocks?.total;
  if (partTotal === undefined || wholeTotal === undefined) return null;
  if (wholeTotal <= 0) return { unfit: `No ${inSentence(pair.whole.label)} in the window.` };
  const short = part ? shortfall(part, context) : null;
  return {
    label: `${pair.part.label} as a share of ${inSentence(pair.whole.label)}, ${short ? short.label : context.window}`,
    value: `${figure((partTotal / wholeTotal) * 100)}%`,
    detail: [`${figure(partTotal)} of ${figure(wholeTotal)}`, pair.byRule ? `${inSentence(pair.whole.label)} taken as the whole` : null, short?.note]
      .filter(Boolean)
      .join(" · "),
    warn: Boolean(short),
  };
}

/** Whether a request holds a comparing word, with or without a value after it. */
export function comparing(typed: string): boolean {
  return COMPARING.test(typed.toLowerCase());
}

