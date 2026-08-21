/**
 * Arranging the scheduler's counters into the rows the waterfall draws.
 *
 * Kept out of the component so it can be tested without a DOM, in the same way
 * as the turn folding and the bar scale.
 */

import type { Waterfall } from "./types";

/** What a row is doing in the list, which is what decides how it is drawn. */
export type RowKind =
  /** A point every transaction passes through: received, buffered, scheduled. */
  | "stage"
  /** A transaction that got no further, and the reason. */
  | "loss"
  /** Neither: something that happened without anything being lost. */
  | "note";

export interface WaterfallRow {
  key: string;
  label: string;
  kind: RowKind;
  count: number;
  /** Of everything received, in `[0, 1]`. The bar's length. */
  share: number;
  explain: string;
}

/**
 * The rows, in the order a transaction meets them.
 *
 * Always the same rows in the same order, including the ones reading nought.
 * Two reasons. A row that appeared only when it fired would change the card's
 * height under whoever was reading it, which this dashboard has been bitten by
 * more than once. And a zero is worth reading: it is the difference between
 * "no transaction failed its fee payer check" and "nothing here counts that".
 */
export function waterfallRows(w: Waterfall): WaterfallRow[] {
  // Everything is drawn against what arrived, so the bars are comparable down
  // the whole card rather than each stage being renormalised against the one
  // above it. Guarded because the card is drawn from the first sample.
  const of = (count: number) => (w.received > 0 ? count / w.received : 0);
  const row = (
    key: string,
    label: string,
    kind: RowKind,
    count: number,
    explain: string,
  ): WaterfallRow => ({ key, label, kind, count, share: of(count), explain });

  return [
    row(
      "received",
      "Received",
      "stage",
      w.received,
      "Transactions handed to the banking stage after signature verification. Everything below is what became of them.",
    ),

    // Lost at the door. These and `buffered` account for every one of the
    // above exactly — it is an identity the validator's own tests assert.
    row(
      "not_held",
      "forwarding, not held",
      "loss",
      w.not_held,
      "Not this validator's to execute. A node that is not near its leader slot forwards transactions to the one that is rather than buffering them, so on most validators most of the time this is nearly the whole of the traffic. It is the ordinary state of a healthy node, not a fault.",
    ),
    row(
      "check_queue_full",
      "check queue full",
      "loss",
      w.check_queue_full,
      "Arrived faster than the checks could be run. Unlike the row above this one is real loss under load: the transaction was this validator's to take and it was dropped for want of capacity.",
    ),
    row(
      "unparsable",
      "would not parse",
      "loss",
      w.unparsable,
      "Malformed, or failed sanitization. Nothing a validator can do about these and nothing to tune — they are what the network sends.",
    ),
    row(
      "bad_locks",
      "bad account locks",
      "loss",
      w.bad_locks,
      "Asked to lock accounts it could not have — too many, or the same one twice.",
    ),
    row(
      "compute_budget",
      "compute budget",
      "loss",
      w.compute_budget,
      "Its compute budget instructions did not add up.",
    ),
    row(
      "too_old",
      "blockhash too old",
      "loss",
      w.too_old,
      "Its blockhash had aged out, or its durable nonce did not hold. Usually a sender whose transaction sat somewhere too long before reaching here.",
    ),
    row(
      "already_processed",
      "already processed",
      "loss",
      w.already_processed,
      "Already in the ledger. Common and harmless: senders retry, and every retry after the first lands here.",
    ),
    row(
      "fee_payer",
      "fee payer could not pay",
      "loss",
      w.fee_payer,
      "The account meant to pay the fee could not cover it.",
    ),
    row(
      "filtered",
      "filtered out",
      "loss",
      w.filtered,
      "Excluded by this validator's own account key filter, if one is configured.",
    ),
    row(
      "nonce_conflict",
      "nonce conflict",
      "loss",
      w.nonce_conflict,
      "A durable nonce transaction for the same nonce account was already queued at the same or higher priority.",
    ),

    row(
      "buffered",
      "Buffered",
      "stage",
      w.buffered,
      "Passed every check at the door and went into the queue to be scheduled. This plus the losses above is exactly the received count.",
    ),

    // Lost from the queue, having already been buffered.
    row(
      "queue_full",
      "queue full",
      "loss",
      w.queue_full,
      "Pushed out of a full queue by something paying more. The signal that this validator is being offered more work than it has room to hold.",
    ),
    row(
      "nonce_evicted",
      "outranked by a nonce",
      "loss",
      w.nonce_evicted,
      "Removed to make way for a durable nonce transaction on the same account that outranked it.",
    ),
    row(
      "cleared",
      "cleared",
      "loss",
      w.cleared,
      "Thrown away when the queue was cleared, which is what happens at the end of a stretch of leader slots to whatever did not make it into a block.",
    ),
    row(
      "cleaned",
      "cleaned",
      "loss",
      w.cleaned,
      "Thrown away as stale while sitting in the queue.",
    ),

    row(
      "scheduled",
      "Scheduled",
      "stage",
      w.scheduled,
      "Handed to a worker thread to execute. This is not buffered minus the losses above it: the queue holds a standing population, so what is scheduled in this window was largely buffered in an earlier one.",
    ),
    row(
      "blocked_conflicts",
      "held back: account conflicts",
      "note",
      w.blocked_conflicts,
      "Wanted accounts another transaction was already writing, so it waited rather than being lost. High figures mean contention — many transactions after the same accounts at once.",
    ),
    row(
      "blocked_threads",
      "held back: all workers busy",
      "note",
      w.blocked_threads,
      "Nothing wrong with the transaction; every worker thread was occupied. This is the scheduler saying it had work it could not place.",
    ),

    row(
      "finished",
      "Finished",
      "stage",
      w.finished,
      "Came back from a worker completed. Includes transactions that executed and failed — landing in a block having failed is still finishing.",
    ),
    row(
      "retried",
      "sent back to retry",
      "note",
      w.retried,
      "Came back from a worker to be tried again rather than completed, and went back into the queue.",
    ),
  ];
}

/**
 * How much of what this validator kept actually got scheduled.
 *
 * The headline the card leads with, because the received count on its own says
 * more about the cluster than about this node — it is dominated by traffic the
 * validator was never going to execute. Against what it did hold, the figure
 * says whether it is keeping up.
 *
 * Null when it held nothing at all in the window, which is the normal state of
 * a validator that has not been leader recently rather than a failure to
 * schedule.
 */
export function scheduledShare(w: Waterfall): number | null {
  if (w.buffered <= 0) return null;
  // Capped: the two counts are different populations a window apart, so a
  // queue draining faster than it fills genuinely reports more scheduled than
  // buffered, and a figure above 100% reads as a bug rather than as a drain.
  return Math.min(1, w.scheduled / w.buffered);
}
