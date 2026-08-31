import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Empty, Tag, Td, Th } from "../components/DataTable";
import { api } from "../lib/api";
import { money, num, pnlClass, qty as fmtQty, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { Trade, TradesPayload } from "../lib/types";

function Row({ trade }: { trade: Trade }) {
  const gross = num(trade.gross);
  const net = num(trade.net);
  const short = trade.direction === "SHORT";

  return (
    <tr className="border-t" style={{ borderColor: "var(--hairline)" }}>
      <Td align="left" muted>
        {trade.closed_day}
      </Td>
      <Td align="left" muted>
        {trade.account}
      </Td>
      <Td align="left">
        <span className="font-medium">{trade.symbol}</span>
        <Tag title={trade.product_type}>{trade.product_type}</Tag>
      </Td>
      <Td align="left">
        <span
          className={`text-xs font-semibold ${
            short ? "text-[var(--loss)]" : "text-[var(--gain)]"
          }`}
        >
          {trade.direction}
        </span>
        <span className="ml-1.5 text-xs text-[var(--ink-muted)]">{trade.kind}</span>
      </Td>
      <Td>{fmtQty(num(trade.qty))}</Td>
      <Td muted>
        <Money>{money(num(trade.entry_price))}</Money>
      </Td>
      <Td muted>
        <Money>{money(num(trade.exit_price))}</Money>
      </Td>
      <Td tone={pnlClass(gross)}>
        <Money>{signed(gross)}</Money>
      </Td>
      <Td muted>
        {trade.charges === null ? (
          <span title="No charges recorded for the days this trade touched">—</span>
        ) : (
          <Money>{money(num(trade.charges))}</Money>
        )}
      </Td>
      <Td tone={pnlClass(net)}>
        <strong>
          {net === null ? (
            <span className="text-[var(--ink-muted)]" title="Cannot be netted without its charges">
              —
            </span>
          ) : (
            <Money>{signed(net)}</Money>
          )}
        </strong>
      </Td>
    </tr>
  );
}

export function TradesPage() {
  const { hidden, toggle } = usePrivacy();
  const [kind, setKind] = useState<"all" | "intraday" | "positional">("all");
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["trades"],
    queryFn: () => api.get<TradesPayload>("/trades"),
    refetchInterval: 15000,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="trades" />;

  const shown = data.trades.filter((t) => kind === "all" || t.kind === kind);
  const t = data.totals;

  return (
    <>
      <PageHeader
        title="Trades"
        subtitle={
          <>
            {t.trades} closed round trips · gross{" "}
            <span className={pnlClass(num(t.gross))}>{signed(num(t.gross))}</span> · net{" "}
            <span className={pnlClass(num(t.net))}>{signed(num(t.net))}</span>
          </>
        }
        actions={
          <div className="flex gap-2">
            <div className="flex rounded border" style={{ borderColor: "var(--border)" }}>
              {(["all", "intraday", "positional"] as const).map((option) => (
                <button
                  key={option}
                  onClick={() => setKind(option)}
                  className={`px-3 py-1.5 text-sm capitalize ${
                    kind === option ? "bg-black/5 dark:bg-white/10 font-medium" : "text-[var(--ink-secondary)]"
                  }`}
                >
                  {option}
                </button>
              ))}
            </div>
            <button
              onClick={toggle}
              className="rounded border px-3 py-1.5 text-sm"
              style={{ borderColor: "var(--border)" }}
            >
              {hidden ? "Show figures" : "Hide figures"}
            </button>
          </div>
        }
      />

      {/* A net total that silently omits the trades it could not cost is a
          smaller number pretending to be a complete one. */}
      {t.trades_without_charges > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-warning)", color: "var(--ink)" }}
        >
          {t.trades_without_charges} of {t.trades} trades have no charges recorded for the
          days they touched, so they are shown gross and excluded from the net total.
          Import them with <code>scripts/fetch_history.py</code>.
        </div>
      )}

      <Card className="overflow-x-auto">
        {shown.length === 0 ? (
          <Empty what={kind === "all" ? "closed trades yet" : `${kind} trades`} />
        ) : (
          <table className="w-full min-w-[1020px]">
            <thead>
              <tr>
                <Th align="left">Closed</Th>
                <Th align="left">Account</Th>
                <Th align="left">Symbol</Th>
                <Th align="left" help="Held overnight or not — from the days, not the product">
                  Side
                </Th>
                <Th>Qty</Th>
                <Th>Entry</Th>
                <Th>Exit</Th>
                <Th>Gross</Th>
                <Th help="Apportioned from the day's charges by turnover — an estimate">
                  Charges
                </Th>
                <Th>Net</Th>
              </tr>
            </thead>
            <tbody>
              {shown.map((trade, index) => (
                <Row key={`${trade.account}-${trade.symbol}-${trade.closed_day}-${index}`} trade={trade} />
              ))}
            </tbody>
          </table>
        )}
      </Card>

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Matched FIFO from the fills the agents recorded, so per-trade P&amp;L exists from 1
        August onward. Charges are <strong>estimated</strong>: the broker reports them per
        day and per segment, never per symbol, so each trade takes its share of the days it
        touched in proportion to turnover. The exact year-to-date figure is on the
        Portfolio page.
      </p>
    </>
  );
}
