/** The Ask page's answers about this validator's own blocks: which requests mean them, and figures
 *  over the blocks it built and the cluster kept, by the Slots page's own arithmetic. */

import { amount, figure, type Series } from "./ask";
import { lasting, thresholdOf, type Answered, type Operation, type Threshold, type ThresholdRead } from "./askFigures";
import { earningsTotal } from "./earnings";
import { headOf } from "./producedIndex";
import { earnedOf, type BlockHead } from "./produced";
import type { EpochEarnings, FiguresPage, LeaderSlotCounts, TipRates } from "./types";

/** Words that tie a request to our own blocks; never a bare "we" or "us", which a node's own
 *  readings use as well. */
const OWN = [
  /\b(?:our|my)\s+(?:own\s+)?(?:blocks?|slots?|leader\s+slots?|turns?)\b/,
  /\bblocks?\s+(?:we|i)\s+(?:produced|built|made|led)\b/,
  /\b(?:we|i)\s+(?:produced|built|skipped|led)\b/,
  /\b(?:did|do|have)\s+(?:we|i)\s+(?:produce|build|skip|lead|led|skipped|produced|built)\b/,
  /\bas\s+(?:a\s+)?leader\b/,
  /\bwhen\s+we\s+lead\b/,
  /\bours\b/,
  /\b(?:our|my)\s+(?:own\s+)?(?:fees?|tips?|transactions?|compute|failed|priority)/,
];

const EARNING = /\bearn\w*|\bincome\b|\brevenue\b|\bpaying\s+us\b|\bpaid\s+us\b|\b(?:we|i)\s+(?:made|kept)\b/;

const PRODUCTION = /\bproduc\w*|\bskip(?:ped|s)?\b|\bleader\s+slots?\b|\bskip\s+rate\b/;

/** A skip with "we" or "our" anywhere is ours: no other leader's skips are asked that way. */
const SKIP = /\bskip(?:ped|s|ping)?\b/;
const WE = /\b(?:we|us|our|i|my)\b/;

/** Whether a request asks about this validator's own blocks. */
export function ownScope(typed: string): boolean {
  const text = typed.toLowerCase();
  return EARNING.test(text) || OWN.some((pattern) => pattern.test(text)) || (SKIP.test(text) && WE.test(text));
}

/** Whether a request asks what this validator earned. */
export function asksEarned(typed: string): boolean {
  return EARNING.test(typed.toLowerCase());
}

/** Whether a request asks how many of its leader slots this validator produced or skipped. */
export function asksProduction(typed: string): boolean {
  const text = typed.toLowerCase();
  return PRODUCTION.test(text) && ownScope(text);
}

/** The epoch a request names by word, or null where it names none. */
export function epochAsked(typed: string): "this" | "last" | null {
  const text = typed.toLowerCase();
  if (/\b(?:last|previous|prior|past)\s+epoch\b/.test(text)) return "last";
  if (/\b(?:this|current)\s+epoch\b/.test(text)) return "this";
  return null;
}

type OwnKey = "fees" | "priority_fees" | "compute" | "transactions" | "tips";

/** The per-slot series our own blocks answer, each from what the block list holds. */
export const OWN_KEYS: ReadonlySet<string> = new Set<OwnKey>(["fees", "priority_fees", "compute", "transactions", "tips"]);

const isOwnKey = (key: string): key is OwnKey => OWN_KEYS.has(key);

/** What a series means for our blocks, as the Read as line names it. */
const OWN_LABEL: Readonly<Record<OwnKey, { head: string; unit: string; total: string }>> = {
  fees: { head: "Fees earned from our blocks", unit: "SOL", total: "SOL" },
  priority_fees: { head: "Priority fees in our blocks", unit: "SOL", total: "SOL" },
  compute: { head: "Compute in our blocks", unit: "million CU per block", total: "billion CU" },
  transactions: { head: "Transactions in our blocks", unit: "per block", total: "transactions" },
  tips: { head: "Our share of tips in our blocks", unit: "SOL", total: "SOL" },
};

/** A series as the Read as line names it for our blocks. */
export function ownLabel(series: Series): { head: string; unit: string } {
  if (!isOwnKey(series.key)) return { head: series.label, unit: series.unit };
  const label = OWN_LABEL[series.key];
  return { head: label.head, unit: label.unit };
}

/** A value compared against, read in the unit our blocks give: transactions per block, not per second. */
export function ownThresholdOf(typed: string, series: Series): ThresholdRead {
  return thresholdOf(
    typed,
    series.key === "transactions" ? { ...series, key: "own_transactions", label: "transactions in our blocks" } : series,
  );
}

/** A reading as shown, small SOL figures to three significant digits. */
export const ownAmount = amount;

/** One block's reading of a series, in the unit shown, or null where the block list lacks it. */
export function ownValue(key: string, block: BlockHead, rates: TipRates | undefined): number | null {
  switch (key) {
    case "fees":
      return earnedOf(block, rates).total / 1e9;
    case "priority_fees":
      return block.priority_fees / 1e9;
    case "compute":
      return block.block_cost / 1e6;
    case "transactions":
      return block.transactions;
    case "tips": {
      const share = earnedOf(block, rates).tips;
      return share === null ? null : share / 1e9;
    }
    default:
      return null;
  }
}

/** The span our blocks are read over: a time window, or this epoch or the last by slot. */
export type OwnWindow = { kind: "time"; from: number; to: number } | { kind: "epoch"; epoch: number; first: number; last: number };

/** Whether one of our blocks falls in a window. */
export function inWindow(block: BlockHead, window: OwnWindow): boolean {
  if (window.kind === "epoch") return block.slot >= window.first && block.slot <= window.last;
  const time = block.slot_time_millis;
  return time !== null && time >= window.from && time < window.to;
}

/** Pages of the block list read newest first until one reaches back past the window; `reached`
 *  is false where the list ran out first. A refused page fails the whole read. */
export async function readOwnBlocks(
  page: (before: number | undefined) => Promise<FiguresPage | null>,
  window: OwnWindow,
  maxPages = 64,
): Promise<{ blocks: BlockHead[]; reached: boolean }> {
  const blocks: BlockHead[] = [];
  let before: number | undefined;
  let reached = false;
  for (let count = 0; count < maxPages; count += 1) {
    const reply = await page(before);
    if (!reply) throw new Error("the block list was refused");
    let passed = false;
    for (const row of reply.figures) {
      const block = headOf(row);
      if (inWindow(block, window)) blocks.push(block);
      else if (window.kind === "epoch" ? block.slot < window.first : (block.slot_time_millis ?? Infinity) < window.from) passed = true;
    }
    if (passed) reached = true;
    if (passed || reply.next === null) break;
    before = reply.next;
  }
  blocks.sort((a, b) => a.slot - b.slot);
  return { blocks, reached };
}

/** One of our blocks with its reading. */
export interface OwnReading {
  slot: number;
  time: number | null;
  value: number;
}

/** Our blocks' readings of a series, leaving out blocks that lack it. */
export function ownReadings(blocks: readonly BlockHead[], key: string, rates: TipRates | undefined): OwnReading[] {
  return blocks.flatMap((block) => {
    const value = ownValue(key, block, rates);
    return value === null ? [] : [{ slot: block.slot, time: block.slot_time_millis, value }];
  });
}

export interface OwnContext {
  /** The window as the figure names it: "epoch 1093 so far", "past hour". */
  window: string;
  threshold: ThresholdRead;
  clock: (millis: number) => string;
  /** Fewer blocks held than the chain shows produced, said beside the figure. */
  short: string | null;
}

const place = (reading: OwnReading, clock: (millis: number) => string): string =>
  `slot ${figure(reading.slot)}${reading.time === null ? "" : ` at ${clock(reading.time)}`}`;

const holds = (threshold: Threshold, value: number): boolean =>
  threshold.above
    ? threshold.inclusive
      ? value >= threshold.value
      : value > threshold.value
    : threshold.inclusive
      ? value <= threshold.value
      : value < threshold.value;

/** The figure an operation gives over our blocks. */
export function ownFigure(
  operation: Operation | null,
  series: Series,
  held: readonly BlockHead[],
  rates: TipRates | undefined,
  context: OwnContext,
): Answered {
  if (!operation || operation === "chart" || operation === "share" || !isOwnKey(series.key)) return null;
  const readings = ownReadings(held, series.key, rates);
  if (readings.length === 0) return { unfit: ownEmpty(series, held.length, context.window) };
  const label = OWN_LABEL[series.key];
  const blocks = `${figure(readings.length)} of our block${readings.length === 1 ? "" : "s"}`;
  const notes = (...parts: (string | null)[]): string => [...parts, context.short].filter(Boolean).join(" · ");
  const name = label.head.charAt(0).toLowerCase() + label.head.slice(1);
  const total = readings.reduce((sum, reading) => sum + reading.value, 0);
  const warn = context.short !== null;
  switch (operation) {
    case "total": {
      const shown = series.key === "compute" ? total / 1000 : total;
      return {
        label: `${label.head}, ${context.window}`,
        value: `${ownAmount(shown, label.total)} ${label.total}`,
        detail: notes(blocks, series.key === "fees" ? feeParts(held, rates) : null),
        warn,
      };
    }
    case "mean":
      return {
        label: `Mean ${name}, ${context.window}`,
        value: `${ownAmount(total / readings.length, label.unit)} ${perBlock(label.unit)}`,
        detail: notes(blocks),
        warn,
      };
    case "highest":
    case "lowest": {
      const highest = operation === "highest";
      const pick = readings.reduce((best, reading) => ((highest ? reading.value > best.value : reading.value < best.value) ? reading : best));
      return {
        label: `${highest ? "Highest" : "Lowest"} ${name}, ${context.window}`,
        value: `${ownAmount(pick.value, label.unit)} ${perBlock(label.unit)}`,
        detail: notes(place(pick, context.clock)),
        warn,
      };
    }
    case "when_above":
    case "when_below": {
      const threshold = context.threshold;
      if (!threshold) return null;
      if ("wrong" in threshold) return { unfit: threshold.wrong };
      const crossed = readings.filter((reading) => holds(threshold, reading.value));
      const words = threshold.above ? (threshold.inclusive ? "at least" : "over") : threshold.inclusive ? "at most" : "under";
      return {
        label: `${figure(crossed.length)} of our ${figure(readings.length)} blocks ${words} ${ownAmount(threshold.value, label.unit)} ${perBlock(label.unit)}`,
        value: "",
        detail: notes(crossed.length > 0 ? `the latest ${place(crossed[crossed.length - 1], context.clock)}` : null, context.window),
        warn,
      };
    }
    default:
      return null;
  }
}

/** Why a series has no readings over our blocks: none in the window, or none read. */
export function ownEmpty(series: Series, blocks: number, window: string): string {
  if (blocks === 0) return `No blocks of ours, ${window}.`;
  return series.key === "tips" ? "Tips are not read on this build." : NOT_READ;
}

const NOT_READ = "Not read for these blocks.";

/** A unit as said of one block. */
function perBlock(unit: string): string {
  return unit.endsWith("per block") ? unit : `${unit} per block`;
}

/** The parts of the fees our blocks earned, as the Slots page counts them. */
function feeParts(held: readonly BlockHead[], rates: TipRates | undefined): string {
  let base = 0;
  let priority = 0;
  let tips = 0;
  for (const block of held) {
    const earned = earnedOf(block, rates);
    base += earned.base;
    priority += earned.priority;
    tips += earned.tips ?? 0;
  }
  const parts = [`base fees after the burn ${ownAmount(base / 1e9, "SOL")}`, `priority ${ownAmount(priority / 1e9, "SOL")}`];
  if (tips > 0) parts.push(`our share of tips ${ownAmount(tips / 1e9, "SOL")}`);
  return parts.join(", ");
}

/** How many of its leader slots this validator produced in an epoch, as the Slots page's epoch row
 *  counts them; slots too old for the slot history are said apart, not counted as either. */
export function production(counts: LeaderSlotCounts | undefined, context: { window: string; short: string | null }): Answered {
  if (!counts) return { unfit: "The leader slot counts for that epoch are not held." };
  const answered = counts.produced + counts.skipped;
  const unread = counts.passed - answered;
  const toCome = counts.slots - counts.passed;
  return {
    label: `Our leader slots, ${context.window}`,
    value: `${figure(counts.produced)} of ${figure(answered)} produced`,
    detail: [
      counts.skipped === 0 ? "no skips" : `${figure(counts.skipped)} skipped`,
      answered > 0 && counts.skipped > 0 ? `${figure((counts.skipped / answered) * 100)}% skip rate` : null,
      unread > 0 ? `${figure(unread)} too old for the slot history` : null,
      toCome > 0 ? `${figure(toCome)} to come` : null,
      context.short,
    ]
      .filter(Boolean)
      .join(" · "),
    warn: false,
  };
}

/** The This epoch card's voting and producer bonus line, worded as the card words it. */
export function earningsLine(earnings: EpochEarnings | null | undefined, epoch: number | null): Answered {
  if (!earnings || (epoch !== null && earnings.epoch !== epoch)) return null;
  const { kept, rest } = earningsTotal(earnings);
  return {
    label: `Voting and producer bonus, epoch ${figure(earnings.epoch)} so far`,
    value: `${kept} ${rest}`,
    detail: "as the This epoch card counts it, and apart from the fees below",
    warn: false,
  };
}

/** A span of our blocks, as a figure line gives it. */
export function heldSpan(blocks: readonly BlockHead[]): string | null {
  const first = blocks[0]?.slot_time_millis;
  const last = blocks[blocks.length - 1]?.slot_time_millis;
  return first !== undefined && last !== undefined && first !== null && last !== null ? lasting(last - first) : null;
}
