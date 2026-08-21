import { count, percent } from "../format";
import { waterfallRows } from "../waterfall";
import type { Waterfall } from "../types";
import { Explain } from "./primitives";

/**
 * The waterfall itself, drawn the same way wherever it appears.
 *
 * Two places want it: the live card, over a rolling window, and the expanded
 * detail of a produced block, over that one slot. They differ in what they are
 * counting and in nothing else, so the rows, the bars and the wording are one
 * component rather than two that have to be kept saying the same thing.
 */
export function WaterfallRows({ waterfall }: { waterfall: Waterfall }) {
  return (
    <div className="waterfall">
      {waterfallRows(waterfall).map((row) => (
        <div key={row.key} className={`waterfall-row is-${row.kind}`}>
          <Explain text={row.explain} className="waterfall-label">
            {row.label}
          </Explain>
          <span className="waterfall-count">{count(row.count)}</span>
          <span className="waterfall-bar" aria-hidden="true">
            {/* Floored above nought so a row that fired at all leaves a mark
                rather than rounding away to an empty track, which reads the
                same as not having fired. */}
            <span
              className="waterfall-fill"
              style={{ width: `${row.count > 0 ? Math.max(1, row.share * 100) : 0}%` }}
            />
          </span>
          <span className="waterfall-share">{row.count > 0 ? percent(row.share, 1) : ""}</span>
        </div>
      ))}
    </div>
  );
}
