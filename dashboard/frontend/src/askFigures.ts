/** Figures the Ask page works out beside a chart: a total, a mean, a peak, a share, or when a series
 *  was over or under a value. The model only names the operation; the value is read here. */

import { figure, namesSeries, SERIES, type Bins, type Series } from "./ask";

export const OPERATIONS = ["chart", "total", "mean", "highest", "lowest", "share", "when_above", "when_below"] as const;

export type Operation = (typeof OPERATIONS)[number];

/** As measured on the built-in model; a test holds it to that text, line by line. */
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

/** The part and whole of a share among the picked series, in either order. */
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

/** Series read in time, for which more is worse. */
const TIMED = new Set(["replay", "fill"]);

/** The operation for one series, set right where the model read a word of judgement as a direction:
 *  the worst, slowest or longest of a time is its highest, the best, fastest or shortest its lowest. */
export function directed(operation: Operation | null, typed: string, series: Series): Operation | null {
  if ((operation !== "highest" && operation !== "lowest") || !TIMED.has(series.key)) return operation;
  const text = typed.toLowerCase();
  if (/\b(?:worst|slowest|longest)\b/.test(text)) return "highest";
  if (/\b(?:best|fastest|quickest|shortest)\b/.test(text)) return "lowest";
  return operation;
}

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
  [/^(?:us|µs|micros(?:econds?)?|microseconds?)$/, (value) => value / 1000],
];
const PERCENT_UNITS: Units = [[/^(?:%|percent|pct)$/, SAME]];
const MEMORY_UNITS: Units = [
  [/^(?:gib|gigs?|g)$/, SAME],
  [/^(?:gb|gigabytes?)$/, (value) => (value * 1e9) / 2 ** 30],
  [/^(?:mib)$/, (value) => value / 1024],
  [/^(?:mb|megabytes?)$/, (value) => (value * 1e6) / 2 ** 30],
  [/^(?:tib)$/, (value) => value * 1024],
  [/^(?:tb|terabytes?)$/, (value) => (value * 1e12) / 2 ** 30],
];
const SECOND = "(?:\\/s| per second| a second)?";
const NETWORK_UNITS: Units = [
  [new RegExp(`^(?:kbps|kbit${SECOND}|kilobits?${SECOND})$`), (value) => value / 8000],
  [new RegExp(`^(?:mbps|mbit${SECOND}|megabits?${SECOND})$`), (value) => value / 8],
  [new RegExp(`^(?:gbps|gbit${SECOND}|gigabits?${SECOND})$`), (value) => value * 125],
  [new RegExp(`^(?:kb${SECOND}|kilobytes?${SECOND})$`), (value) => value / 1000],
  [new RegExp(`^(?:mb${SECOND}|megabytes?${SECOND})$`), SAME],
  [new RegExp(`^(?:gb${SECOND}|gigabytes?${SECOND})$`), (value) => value * 1000],
];
const RATE_UNITS: Units = [
  [/^(?:per second|a second|each second|\/s|tps|pps)$/, SAME],
  [/^(?:per minute|a minute|each minute|\/min)$/, (value) => value / 60],
];
const BLOCK_UNITS: Units = [[/^(?:per block|a block|each block)$/, SAME]];
const SOL_UNITS: Units = [
  [/^(?:sol)$/, SAME],
  [/^(?:lamports?)$/, (value) => value / 1e9],
  ...BLOCK_UNITS,
];
const COMPUTE_UNITS: Units = [[/^(?:million|million cu|mcu)$/, SAME], ...BLOCK_UNITS];

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
    case "failed":
    case "repaired":
    case "recovered":
      return BLOCK_UNITS;
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

/** Inclusive forms first, so "at least" is not read as a bare comparison. */
const ABOVE_INCLUSIVE = "at least|no less than|not less than|not under|not below|>=|≥";
const BELOW_INCLUSIVE = "at most|no more than|not more than|not over|not above|<=|≤";
const ABOVE = `${ABOVE_INCLUSIVE}|over|above|more than|greater than|higher than|longer than|slower than|larger than|bigger than|exceed(?:s|ed|ing)?|beyond|went over|went above|>`;
const BELOW = `${BELOW_INCLUSIVE}|under|below|less than|lower than|fewer than|shorter than|faster than|smaller than|dipped (?:below|under)|fell (?:below|under)|dropped (?:below|under)|<`;
/** A window, never a value: "over the past 2 hours", "under 30 minutes". */
const WINDOW_UNIT = /^(?:m|mins?|minutes?|h|hrs?|hours?|d|days?|w|wks?|weeks?)$/;
const NUMBER = "[-+]?(?:\\d[\\d,]*(?:\\.\\d+)?|\\.\\d+)";
const UNIT_WORDS = "%|(?:per|a|each)\\s+(?:second|minute|block)|[a-zµ/]+(?:\\s+(?:per|a)\\s+(?:second|minute))?";
/** No word character just before a comparing word, so "dover" is not "over"; a sign may follow one. */
const COMPARISON = new RegExp(`(?:^|[^a-z0-9]|(?=[<>≤≥]))(${ABOVE}|${BELOW})\\s*(${NUMBER})(\\s*)(${UNIT_WORDS})?`, "g");
/** Comparing words with no number after them, so the answer can ask for one. */
const COMPARING = new RegExp(`(?:^|[^a-z0-9]|(?=[<>≤≥]))(?:${ABOVE}|${BELOW})(?:$|[^a-z])`);

export interface Threshold {
  above: boolean;
  /** At least or at most, where the value itself counts. */
  inclusive: boolean;
  /** In the series' own unit. */
  value: number;
  /** The words in the request it was read from, to take out before the window is read. */
  matched: string;
}

export type ThresholdRead = Threshold | { wrong: string } | null;

/** The value a request compares a series against, in that series' unit; `wrong` for a value in a
 *  unit the series is not read in, null for none. With several comparisons, the one after the
 *  series' own name is taken first. */
export function thresholdOf(typed: string, series: Series): ThresholdRead {
  const text = typed.toLowerCase();
  let after = 0;
  let named: Threshold | null = null;
  let fitting: Threshold | null = null;
  let wrong: ThresholdRead = null;
  for (const found of text.matchAll(COMPARISON)) {
    const at = found.index ?? 0;
    const lead = text.slice(after, at);
    after = at + found[0].length;
    const read = comparison(found, series);
    if (!read) continue;
    if ("wrong" in read) {
      wrong ??= read;
      continue;
    }
    if (!named && leads(lead, series)) named = read;
    fitting ??= read;
  }
  return named ?? fitting ?? wrong;
}

/** Whether the words before a comparison name `series` itself, not a longer name holding its own,
 *  as "failed transactions" holds "transactions". */
function leads(lead: string, series: Series): boolean {
  if (!namesSeries(lead, series)) return false;
  return !SERIES.some(
    (other) => other !== series && namesSeries(lead, other) && namesSeries(`${other.label} ${other.hint}`, series),
  );
}

function unitShown(number: string, unit: string): string {
  return unit === "%" ? `${number}%` : `${number} ${unit}`;
}

/** One comparing phrase read for `series`, or null where it names a window instead. */
function comparison(found: RegExpMatchArray, series: Series): ThresholdRead {
  const [whole, comparator, number, gap, rawUnit] = found;
  let unit = (rawUnit ?? "").trim();
  const glued = Boolean(rawUnit) && gap === "";
  if (WINDOW_UNIT.test(unit)) return null;
  // A second is a window for every series but those read in time.
  const timed = series.key === "replay" || series.key === "fill";
  if (!timed && /^(?:s|secs?|seconds?)$/.test(unit)) return null;
  let value = Number(number.replace(/,/g, ""));
  if (!Number.isFinite(value)) return null;
  const thousands = /^(?:k|thousand)(?:\s+|$)/.exec(unit);
  if (thousands) {
    value *= 1000;
    unit = unit.slice(thousands[0].length).trim();
  }
  const above = new RegExp(`^(?:${ABOVE})$`).test(comparator);
  const inclusive = new RegExp(`^(?:${ABOVE_INCLUSIVE}|${BELOW_INCLUSIVE})$`).test(comparator);
  const label = inSentence(series.label);
  const notIn = (): ThresholdRead => ({
    wrong: `${unitShown(number, unit)} does not fit ${label}, which is read in ${UNIT[series.key] || "plain numbers"}.`,
  });
  let factor = SAME;
  if (unit) {
    const fits = unitsOf(series.key).find(([pattern]) => pattern.test(unit));
    if (fits) factor = fits[1];
    // A unit hard against the digits that is none of ours is not a bare number.
    else if (glued || ANY_UNIT.some(([pattern]) => pattern.test(unit)) || /^per block$/.test(unit)) return notIn();
    // A word after the number that is no unit, as in "over 50 in the last hour".
    else unit = "";
  }
  let read = factor(value);
  // A bare figure for compute over a thousand is in units, not millions: no block uses a billion.
  if (series.key === "compute" && !unit && value > 1000) read = value / 1e6;
  const kept = unit ? whole : whole.slice(0, whole.lastIndexOf(number) + number.length);
  return { above, inclusive, value: read, matched: kept.trimStart() };
}

/** The request with its comparison taken out, so "under 5 s" is not read as a window. */
export function withoutThreshold(typed: string, series: Series): string {
  const read = thresholdOf(typed, series);
  if (!read || "wrong" in read) return typed;
  const at = typed.toLowerCase().indexOf(read.matched);
  return at < 0 ? typed : `${typed.slice(0, at)} ${typed.slice(at + read.matched.length)}`;
}

/** Whether a value is on the side asked for. */
export function holds(threshold: Threshold, value: number): boolean {
  if (threshold.above) return threshold.inclusive ? value >= threshold.value : value > threshold.value;
  return threshold.inclusive ? value <= threshold.value : value < threshold.value;
}

export interface Span {
  start: number;
  end: number;
}

/** Runs of spans where `test` holds, each from its first span's start to its last span's end, held
 *  to `to`. A span with no reading ends a run. */
export function spansOf(
  values: readonly (number | null)[],
  from: number,
  binMillis: number,
  test: (value: number) => boolean,
  to = Infinity,
): Span[] {
  const spans: Span[] = [];
  let open: number | null = null;
  const close = (index: number): void => {
    if (open === null) return;
    spans.push({ start: from + open * binMillis, end: Math.min(to, from + index * binMillis) });
    open = null;
  };
  values.forEach((value, index) => {
    const yes = value !== null && test(value);
    if (yes && open === null) open = index;
    if (!yes) close(index);
  });
  close(values.length);
  return spans;
}

/** A length of time as a figure line gives it. */
export function lasting(millis: number): string {
  const seconds = millis / 1000;
  if (seconds < 10) return `${figure(Math.round(seconds * 10) / 10)} s`;
  if (seconds < 60) return `${Math.round(seconds)} s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours} h ${rest} min` : `${hours} h`;
}

/** What a panel was read as: the operation and series, then the unit. */
export function readAs(operation: Operation | null, series: Series, threshold: ThresholdRead): { head: string; unit: string } {
  const unit = UNIT[series.key] ?? "";
  const name = inSentence(series.label);
  switch (operation) {
    case "total":
      return unfit(operation, series) ? { head: series.label, unit } : { head: `Total of ${name}`, unit: TOTALS[series.key] ?? "" };
    case "mean":
      return { head: `Mean of ${name}`, unit };
    case "highest":
      return { head: `Highest ${name}`, unit };
    case "lowest":
      return { head: `Lowest ${name}`, unit };
    case "when_above":
    case "when_below":
      return threshold && !("wrong" in threshold)
        ? { head: `When ${name} was ${side(threshold)} ${withUnit(threshold.value, series.key)}`, unit: "" }
        : { head: series.label, unit };
    default:
      return { head: series.label, unit };
  }
}

function side(threshold: Threshold): string {
  if (threshold.above) return threshold.inclusive ? "at least" : "over";
  return threshold.inclusive ? "at most" : "under";
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
  threshold: ThresholdRead;
  /** The request held a comparing word. */
  comparing: boolean;
  clock: (millis: number) => string;
}

/** How late a window's first reading may come before the figure says the history began inside it. */
const SHORTFALL_SLACK_MILLIS = 15_000;

/** Where the window's readings begin, when well after it opens: a figure over less than was asked. */
function shortfall(bins: Bins, context: FigureContext): { label: string; note: string } | null {
  if (bins.first <= bins.from + Math.max(bins.binMillis, SHORTFALL_SLACK_MILLIS)) return null;
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

/** A per-slot rate's spans that held blocks for all their seconds: not the one the history begins
 *  in, nor one beside a gap, each of which reads low. The newest is always empty and is no gap. */
function wholeSpans(bins: Bins): (number | null)[] {
  const last = bins.values.length - 1;
  const gap = (index: number): boolean => index >= 0 && index < last && bins.values[index] === null;
  const firstIndex = Math.floor((bins.first - bins.from) / bins.binMillis);
  return bins.values.map((value, index) =>
    value === null || gap(index - 1) || gap(index + 1) || (bins.first > bins.from && index === firstIndex) ? null : value,
  );
}

/** A per-block series' unit, read per block. */
const perBlock = (key: string): string => (UNIT[key] === "per block" ? "per block" : `${UNIT[key]} per block`);

const plural = (count: number, one: string, many: string): string => `${figure(count)} ${count === 1 ? one : many}`;

/** The figure an operation gives for one series. */
export function answerFigure(operation: Operation | null, series: Series, bins: Bins | null, context: FigureContext): Answered {
  if (!operation || operation === "chart" || operation === "share" || !bins) return null;
  const why = unfit(operation, series);
  if (why) return { unfit: why };
  const short = shortfall(bins, context);
  const over = short ? short.label : context.window;
  const name = inSentence(series.label);
  const notes = (...parts: (string | null)[]): string => [...parts, short?.note ?? null].filter(Boolean).join(" · ");
  const rate = series.source === "slots" && series.rate;
  const blocks = series.source === "slots" ? bins.blocks : undefined;
  const span = lasting(bins.binMillis);
  const at = (index: number): number => bins.from + index * bins.binMillis;
  const warn = Boolean(short);
  // A rate's figures leave out spans that held blocks for only part of their seconds.
  const drawn = rate ? wholeSpans(bins) : bins.values;
  switch (operation) {
    case "total": {
      if (series.source === "host") {
        const { total, near } = hostTotal(bins, series.key);
        return {
          label: `Total ${name}, ${over}`,
          value: `${near ? "about " : ""}${figure(total)} ${TOTALS[series.key]}`,
          detail: notes(`from readings in ${span} steps`),
          warn,
        };
      }
      if (!blocks) return null;
      const total = series.key === "compute" ? blocks.total / 1000 : blocks.total;
      const whose = series.key === "fees" || series.key === "priority_fees" || series.key === "tips" ? " from every leader" : "";
      return {
        label: `Total ${name}, ${over}`,
        value: `${figure(total)} ${TOTALS[series.key]}`,
        detail: notes(`${plural(blocks.count, "block", "blocks")}${whose}`),
        warn,
      };
    }
    case "mean": {
      if (blocks && !rate) {
        return { label: `Mean ${name}, ${over}`, value: `${figure(blocks.mean)} ${perBlock(series.key)}`, detail: notes(plural(blocks.count, "block", "blocks")), warn };
      }
      const real = drawn.filter((value): value is number => value !== null);
      if (real.length === 0) return null;
      const mean = real.reduce((sum, value) => sum + value, 0) / real.length;
      return {
        label: `Mean ${name}, ${over}`,
        value: withUnit(mean, series.key),
        detail: notes(series.source === "host" ? `from readings in ${span} steps` : `over the ${span} spans drawn`),
        warn,
      };
    }
    case "highest":
    case "lowest": {
      const highest = operation === "highest";
      const word = highest ? "Highest" : "Lowest";
      if (blocks && !rate) {
        const block = highest ? blocks.high : blocks.low;
        const where = block.slot === null ? `at ${context.clock(block.time)}` : `slot ${figure(block.slot)} at ${context.clock(block.time)}`;
        return { label: `${word} ${name}, ${over}`, value: withUnit(block.value, series.key), detail: notes(where), warn };
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
            warn,
          };
        }
      }
      let index = -1;
      drawn.forEach((value, place) => {
        if (value === null) return;
        const held = index < 0 ? null : drawn[index];
        if (held === null || held === undefined || (highest ? value > held : value < held)) index = place;
      });
      if (index < 0) return null;
      return {
        label: `${word} ${name}, ${over}`,
        value: withUnit(drawn[index] ?? 0, series.key),
        detail: notes(`${span} ${series.source === "host" ? "step" : "average"}, at ${context.clock(at(index))}`),
        warn,
      };
    }
    case "when_above":
    case "when_below": {
      const threshold = context.threshold;
      if (!threshold) {
        return context.comparing ? { unfit: 'No value to compare against; ask, for example, "over 20 ms".' } : null;
      }
      if ("wrong" in threshold) return { unfit: threshold.wrong };
      const words = side(threshold);
      // A rate's spans leave out those that held blocks for only part of their seconds.
      const spans = spansOf(drawn, bins.from, bins.binMillis, (value) => holds(threshold, value), bins.to);
      const mark = { threshold: threshold.value, spans };
      const by = `by ${span} ${series.source === "host" ? "steps" : "averages"}`;
      // Counted only where the blocks were counted against this very value.
      const counted =
        blocks && !rate && blocks.threshold === threshold.value && blocks.inclusive === threshold.inclusive
          ? (threshold.above ? blocks.over : blocks.under)
          : null;
      const value = withUnit(threshold.value, series.key);
      const countLine =
        counted === null || !blocks
          ? null
          : `${figure(counted)} of ${plural(blocks.count, "block", "blocks")} ${counted === 1 ? "was" : "were"} ${words} ${value}`;
      let peaked: number | null = null;
      if (series.source === "host" && threshold.above && bins.peaks) {
        peaked = bins.peaks.filter((value) => value !== null && holds(threshold, value)).length;
      }
      const peakLine = peaked === null ? null : `${plural(peaked, "step", "steps")} had a one-second reading ${words} ${value}`;
      if (spans.length === 0) {
        const real = drawn.filter((one): one is number => one !== null);
        const extreme = real.length === 0 ? null : threshold.above ? Math.max(...real) : Math.min(...real);
        // "Never" only where no single block nor one-second reading crossed either.
        const block = blocks && !rate ? (threshold.above ? blocks.high : blocks.low) : null;
        const never = !(block && holds(threshold, block.value)) && !peaked;
        const where = short ? short.label : `in the ${context.window}`;
        return {
          label: never
            ? `Never ${words} ${value} ${where}`
            : `No ${span} ${series.source === "host" ? "step" : "average"} ${words} ${value} ${where}`,
          value: "",
          detail: notes(
            extreme === null ? null : `${threshold.above ? "highest" : "lowest"} ${span} average ${withUnit(extreme, series.key)}`,
            countLine,
            peakLine,
          ),
          warn,
          mark,
        };
      }
      const lengths = spans.map((one) => one.end - one.start);
      const longest = spans[lengths.indexOf(Math.max(...lengths))];
      const all = lengths.reduce((sum, length) => sum + length, 0);
      return {
        label: `${words.charAt(0).toUpperCase()}${words.slice(1)} ${value} in ${plural(spans.length, "span", "spans")}, ${lasting(all)} in all`,
        value: "",
        detail: notes(`longest ${context.clock(longest.start)}–${context.clock(longest.end)}`, by, countLine, peakLine),
        warn,
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
