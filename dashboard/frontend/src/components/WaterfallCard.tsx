import { percent } from "../format";
import type { Waterfall } from "../types";
import { useStore } from "../useStore";
import { scheduledShare } from "../waterfall";
import { Card, Explain } from "./primitives";
import { WaterfallRows } from "./WaterfallRows";

/**
 * Where the transactions this validator was handed actually went.
 *
 * Named for the scheduler rather than for the TPU, which is what the equivalent
 * on the Firedancer dashboard is called. Theirs begins at the socket and counts
 * everything lost to signature verification and deduplication on the way; this
 * begins where the scheduler is handed what survived all of that, so it is the
 * last stretch of the same journey rather than the whole of it. A title
 * claiming the TPU would invite the two to be read as the same figures.
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

  const kept = scheduledShare(waterfall);

  return (
    <Card title="Scheduler Waterfall" className="waterfall-body">
      <div className="waterfall-headline">
        <Explain text="Of the transactions this validator kept rather than forwarded, the share it managed to hand to a worker. The received count is a poor headline on its own — it is dominated by traffic the node was never going to execute — where this says whether it kept up with what it did take. Absent when it has held nothing recently, which is the ordinary state of a validator that has not been leader.">
          <span className="waterfall-headline-label">Scheduled of held</span>
        </Explain>
        <span className="waterfall-headline-value">
          {kept === null ? "—" : percent(kept, 1)}
        </span>
      </div>

      <WaterfallRows waterfall={waterfall} />

      <div className="card-footnote">
        Five minutes of the banking stage scheduler's own counters. Rows reading
        nought are drawn rather than hidden, so the card keeps its height and a
        zero can be told from a figure nothing measures.
      </div>
    </Card>
  );
}
