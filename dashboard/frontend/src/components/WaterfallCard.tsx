import { count, percent } from "../format";
import type { Waterfall } from "../types";
import { useStore } from "../useStore";
import { scheduledShare, waterfallRows, type WaterfallRow } from "../waterfall";
import { Card, Explain } from "./primitives";

/**
 * Where the transactions this validator was handed actually went.
 *
 * The scheduler counts all of this itself and reports it once a second with its
 * counters reset as it does, so what is drawn is five minutes of work summed
 * rather than anything it is currently holding. Five minutes rather than one
 * because the interesting half only moves while this node is leader, which for
 * most validators is four slots every couple of minutes.
 *
 * Read it top to bottom. Everything sigverify passed on is `Received`; the
 * indented rows under each stage are what got no further, and the reason. The
 * first stretch is a true account — received is exactly buffered plus those
 * losses — and the later ones are not, because the queue holds a standing
 * population and the three stages are three overlapping populations rather than
 * one cohort walking through.
 */
export function WaterfallCard() {
  const store = useStore();
  const waterfall = store.get<Waterfall>("summary", "waterfall");
  if (!waterfall) return null;

  const rows = waterfallRows(waterfall);
  const kept = scheduledShare(waterfall);

  return (
    <Card title="Transaction Waterfall" className="waterfall-body">
      <div className="waterfall-headline">
        <Explain text="Of the transactions this validator kept rather than forwarded, the share it managed to hand to a worker. The received count is a poor headline on its own — it is dominated by traffic the node was never going to execute — where this says whether it kept up with what it did take. Absent when it has held nothing recently, which is the ordinary state of a validator that has not been leader.">
          <span className="waterfall-headline-label">Scheduled of held</span>
        </Explain>
        <span className="waterfall-headline-value">
          {kept === null ? "—" : percent(kept, 1)}
        </span>
      </div>

      <div className="waterfall">
        {rows.map((row) => (
          <Row key={row.key} row={row} />
        ))}
      </div>

      <div className="card-footnote">
        Five minutes of the banking stage scheduler's own counters. Rows reading
        nought are drawn rather than hidden, so the card keeps its height and a
        zero can be told from a figure nothing measures.
      </div>
    </Card>
  );
}

/**
 * One row: a stage, a loss under it, or a note.
 *
 * The bar is always a share of what was received, never of the stage above it,
 * so lengths are comparable the whole way down instead of each section being
 * renormalised against its own heading.
 */
function Row({ row }: { row: WaterfallRow }) {
  return (
    <div className={`waterfall-row is-${row.kind}`}>
      <Explain text={row.explain} className="waterfall-label">
        {row.label}
      </Explain>
      <span className="waterfall-count">{count(row.count)}</span>
      <span className="waterfall-bar" aria-hidden="true">
        {/* Floored above nought so that a row which fired at all leaves a mark
            rather than rounding away to an empty track, which reads the same as
            not having fired. */}
        <span
          className="waterfall-fill"
          style={{ width: `${row.count > 0 ? Math.max(1, row.share * 100) : 0}%` }}
        />
      </span>
      <span className="waterfall-share">{row.count > 0 ? percent(row.share, 1) : ""}</span>
    </div>
  );
}
