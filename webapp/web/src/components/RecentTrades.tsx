import { useQuery } from "@tanstack/react-query";

import { Card } from "./ui";
import { Empty, Tag, Th } from "./DataTable";
import { api } from "../lib/api";
import { money, num, pnlClass, qty as fmtQty, signed } from "../lib/format";
import { Money } from "../lib/privacy";
import type { RecentLine, RecentPayload } from "../lib/types";

/** What has been traded lately — open and closed in one list.
 *
 *  On the order pad because that is where the question comes up: before placing
 *  anything, what am I already in, and what did the last few trades do. Split
 *  across Positions and Trades it takes two pages and a mental join.
 *
 *  The consolidation is the server's: one line per scrip, at the price actually
 *  paid across every fill behind it. This renders what it is given, in the order
 *  it is given, so nothing here can reorder while it is being read.
 */
const LIMIT = 20;

function Line({ line, onPick }: { line: RecentLine; onPick?: () => void }) {
  const net = num(line.net);
  const pnl = net ?? num(line.gross);
  const pct = num(line.pct);
  const open = line.state === "open";

  return (
    <tr
      onClick={onPick}
      className={`border-t ${onPick ? "cursor-pointer hover:bg-black/[0.03] dark:hover:bg-white/[0.04]" : ""}`}
      style={{ borderColor: "var(--hairline)" }}
      title={onPick ? "Load this account and symbol into the pad" : undefined}
    >
      <td className="px-3 py-1.5 text-sm text-[var(--ink-muted)]">{line.account}</td>
      <td className="px-3 py-1.5 text-sm font-medium">
        {line.symbol}
        {/* One position held across both of the broker's books. Worth saying:
            it is the difference between selling today and waiting to settle. */}
        {line.product_type.includes("+") && <Tag title="Held in both books">{line.product_type}</Tag>}
        {line.fills > 1 && (
          <span
            className="ml-1.5 text-[10px] text-[var(--ink-muted)]"
            title={`${line.fills} fills, shown at the average price across them`}
          >
            ×{line.fills}
          </span>
        )}
      </td>
      <td className="px-3 py-1.5">
        <span
          className={`text-xs font-semibold ${
            line.direction === "LONG" ? "text-[var(--gain)]" : "text-[var(--loss)]"
          }`}
        >
          {line.direction}
        </span>
      </td>
      <td className="tnum px-3 py-1.5 text-right text-sm">{fmtQty(num(line.qty))}</td>
      <td className="tnum px-3 py-1.5 text-right text-sm text-[var(--ink-muted)]">
        <Money>{money(num(line.entry_price))}</Money>
      </td>
      <td className="tnum px-3 py-1.5 text-right text-sm">
        <Money>{money(num(line.exit_price))}</Money>
      </td>
      <td className={`tnum px-3 py-1.5 text-right text-sm ${pnlClass(pnl)}`}>
        <strong>
          <Money>{signed(pnl)}</Money>
        </strong>
        {pct !== null && (
          <span className="ml-1 text-xs opacity-70">
            ({pct > 0 ? "+" : pct < 0 ? "−" : ""}
            {Math.abs(pct).toFixed(2)}%)
          </span>
        )}
        {!open && net === null && (
          <span
            className="ml-1 text-xs text-[var(--ink-muted)]"
            title="Gross — the day's charges are not in yet"
          >
            gross
          </span>
        )}
      </td>
      <td className="px-3 py-1.5 text-right text-xs text-[var(--ink-muted)]">
        {open ? (
          <span className="font-medium text-[var(--ink-secondary)]">
            open{line.day && ` · since ${line.day}`}
          </span>
        ) : (
          <>
            {line.day}
            {line.trade_kind && (
              <span className="ml-1 opacity-70">{line.trade_kind}</span>
            )}
          </>
        )}
      </td>
    </tr>
  );
}

export function RecentTrades({ onPick }: { onPick?: (account: string, symbol: string) => void }) {
  const { data } = useQuery({
    queryKey: ["recent"],
    queryFn: () => api.get<RecentPayload>(`/recent?limit=${LIMIT}`),
    refetchInterval: 5000,
  });

  const lines = data?.lines ?? [];
  const open = lines.filter((line) => line.state === "open").length;

  return (
    <Card className="mt-4 overflow-x-auto">
      <div
        className="flex items-baseline gap-2 border-b px-4 py-2.5"
        style={{ borderColor: "var(--hairline)" }}
      >
        <span className="text-sm font-semibold">Recent</span>
        <span className="text-xs text-[var(--ink-muted)]">
          {open} open, and the {LIMIT} most recent closes — one line per scrip, newest first
        </span>
      </div>

      {lines.length === 0 ? (
        <Empty what="trades yet" />
      ) : (
        <table className="w-full min-w-[760px]">
          <thead>
            <tr>
              <Th align="left">Account</Th>
              <Th align="left">Symbol</Th>
              <Th align="left">Side</Th>
              <Th>Qty</Th>
              <Th help="Average paid across every fill behind this line">In</Th>
              <Th help="The mark on an open line, the average got on a closed one">Out</Th>
              <Th help="Unrealised while open, realised net of charges once closed">
                P&amp;L
              </Th>
              <Th align="right">State</Th>
            </tr>
          </thead>
          <tbody>
            {lines.map((line) => (
              <Line
                key={`${line.state}-${line.account}-${line.symbol}-${line.day}-${line.direction}-${line.trade_kind}`}
                line={line}
                onPick={onPick ? () => onPick(line.account, line.symbol) : undefined}
              />
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}
