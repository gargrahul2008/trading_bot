import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Empty, Tag, Td, Th } from "../components/DataTable";
import { Matrix, MatrixEmpty } from "../components/Matrix";
import type { MatrixRow, TotalRow } from "../components/Matrix";
import { api } from "../lib/api";
import { age, money, pnlClass, qty as fmtQty, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { Position, PositionsPayload } from "../lib/types";

const REFRESH_MS = 3000;

/** What a position actually is, in one word.
 *
 *  A negative CNC equity position is stock sold out of holdings awaiting
 *  settlement — not a short anyone has to buy back. Calling it SHORT would show
 *  open risk that does not exist.
 */
function describe(position: Position): { label: string; tone: string } {
  if (position.delivery_sale) return { label: "SOLD", tone: "text-[var(--ink-secondary)]" };
  if (position.net_qty > 0) return { label: "LONG", tone: "text-[var(--gain)]" };
  return { label: "SHORT", tone: "text-[var(--loss)]" };
}

function Row({ position }: { position: Position }) {
  const { label, tone } = describe(position);
  const move =
    position.avg_price > 0
      ? ((position.ltp - position.avg_price) / position.avg_price) * 100 *
        (position.net_qty < 0 ? -1 : 1)
      : null;

  return (
    <tr
      className="border-t"
      style={{ borderColor: "var(--hairline)", opacity: position.from_store ? 0.62 : 1 }}
    >
      <Td align="left" muted>
        {position.account}
      </Td>
      <Td align="left">
        <span className="font-medium">{position.symbol}</span>
        {position.book === "holding" && (
          <Tag title="Settled delivery stock, held in the holdings book">holding</Tag>
        )}
        {position.is_derivative && <Tag title="Derivatives segment">F&amp;O</Tag>}
        {position.carried && <Tag title="Carried in from a previous day">carried</Tag>}
        {position.delivery_sale && (
          <Tag title="Sold out of holdings, awaiting settlement — not a short">
            delivery
          </Tag>
        )}
      </Td>
      <Td align="left">
        <span className={`text-xs font-semibold ${tone}`}>{label}</span>
        <span className="ml-1.5 text-xs text-[var(--ink-muted)]">
          {position.product_type}
        </span>
      </Td>
      <Td>{fmtQty(Math.abs(position.net_qty))}</Td>
      <Td muted>
        <Money>{money(position.avg_price)}</Money>
      </Td>
      <Td>
        <Money>{money(position.ltp)}</Money>
      </Td>
      <Td tone={pnlClass(move)}>
        {move === null ? "—" : `${move > 0 ? "+" : move < 0 ? "−" : ""}${Math.abs(move).toFixed(2)}%`}
      </Td>
      <Td tone={pnlClass(position.unrealised)}>
        <strong>
          <Money>{signed(position.unrealised)}</Money>
        </strong>
      </Td>
      <Td muted>{position.stale ? age(position.age_s) : ""}</Td>
    </tr>
  );
}

/** One row per symbol, one column per account.
 *
 *  A name held in three accounts is one line rather than three, which is the
 *  whole reason to look at six accounts on one screen.
 */
function toMatrix(positions: Position[], accounts: string[]) {
  const bySymbol = new Map<string, Position[]>();
  for (const position of positions) {
    const list = bySymbol.get(position.symbol) ?? [];
    list.push(position);
    bySymbol.set(position.symbol, list);
  }

  const rows: MatrixRow[] = [];
  for (const [symbol, held] of bySymbol) {
    const cells: Record<string, number | null | undefined> = {};
    for (const position of held) {
      cells[position.account] = (cells[position.account] ?? 0) + position.unrealised;
    }
    const first = held[0];
    rows.push({
      key: symbol,
      label: symbol,
      note: first.delivery_sale
        ? "sold"
        : first.is_derivative
          ? "F&O"
          : held.every((p) => p.book === "holding")
            ? "holding"
            : undefined,
      cells,
      total: held.reduce((sum, p) => sum + p.unrealised, 0),
      // Quantities differ per account, so they belong in the hover rather than
      // in a cell that has to hold one number.
      title: held
        .map((p) =>
          `${p.account}: ${p.direction} ${Math.abs(p.net_qty)} @ ${p.avg_price ?? "?"}` +
          (p.book === "holding" ? " (holding)" : ""))
        .join("\n"),
    });
  }

  const totals: TotalRow[] = [];
  const per = (pick: (p: Position) => number) => {
    const values: Record<string, number | null> = {};
    for (const account of accounts) {
      const mine = positions.filter((p) => p.account === account);
      values[account] = mine.length ? mine.reduce((s, p) => s + pick(p), 0) : null;
    }
    return values;
  };
  const cost = (p: Position) => Math.abs(p.net_qty) * (p.avg_price || p.ltp || 0);
  const value = (p: Position) => Math.abs(p.net_qty) * (p.ltp || p.avg_price || 0);

  const costs = per(cost);
  const values = per(value);
  const unreal = per((p) => p.unrealised);
  const pct: Record<string, number | null> = {};
  for (const account of accounts) {
    const c = costs[account];
    pct[account] = c ? ((unreal[account] ?? 0) / c) * 100 : null;
  }
  const costTotal = positions.reduce((s, p) => s + cost(p), 0);
  const unrealTotal = positions.reduce((s, p) => s + p.unrealised, 0);

  totals.push({ label: "At cost", values: costs, total: costTotal });
  totals.push({ label: "At market", values: values,
                total: positions.reduce((s, p) => s + value(p), 0) });
  totals.push({ label: "Unrealised", values: unreal, total: unrealTotal, tone: true });
  totals.push({
    label: "Unrealised %", values: pct,
    total: costTotal ? (unrealTotal / costTotal) * 100 : null,
    tone: true, percent: true,
  });

  return { rows, totals };
}

export function PositionsPage() {
  const { hidden, toggle } = usePrivacy();
  const [detail, setDetail] = useState(false);
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["positions"],
    queryFn: () => api.get<PositionsPayload>("/positions"),
    refetchInterval: REFRESH_MS,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="positions" />;

  const rows = data.positions;
  // From the server, not from the rows: an account holding nothing must still
  // appear, as a column of dots.
  const accounts = data.accounts;
  const matrix = toMatrix(rows, accounts);
  const total = rows.reduce((sum, p) => sum + p.unrealised, 0);
  const longs = rows.filter((p) => p.net_qty > 0 && !p.delivery_sale).length;
  const shorts = rows.filter((p) => p.net_qty < 0 && !p.delivery_sale).length;
  const sold = rows.filter((p) => p.delivery_sale).length;
  const holdings = rows.filter((p) => p.book === "holding").length;

  return (
    <>
      <PageHeader
        title="Positions"
        subtitle={
          <>
            {rows.length} open · {longs} long · {shorts} short
            {holdings > 0 && ` · ${holdings} delivery holdings`}
            {sold > 0 && ` · ${sold} sold from holdings`} · unrealised{" "}
            <span className={pnlClass(total)}>{signed(total)}</span>
          </>
        }
        actions={
          <div className="flex gap-2">
            <div className="flex rounded border" style={{ borderColor: "var(--border)" }}>
              {([["grid", false], ["detail", true]] as const).map(([label, on]) => (
                <button
                  key={label}
                  onClick={() => setDetail(on)}
                  className={`px-3 py-1.5 text-sm capitalize ${
                    detail === on
                      ? "bg-black/5 font-medium dark:bg-white/10"
                      : "text-[var(--ink-secondary)]"
                  }`}
                >
                  {label}
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

      {data.accounts_missing.length > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-critical)", color: "var(--ink)" }}
        >
          <strong>{data.accounts_missing.join(", ")}</strong> could not be reached and has
          nothing stored — any position it holds is missing from this list.
        </div>
      )}

      {!detail && (
        <Card>
          {rows.length === 0 ? (
            <MatrixEmpty>Nothing open.</MatrixEmpty>
          ) : (
            <Matrix
              accounts={accounts}
              rows={matrix.rows}
              totals={matrix.totals}
              labelHeader="Symbol"
            />
          )}
        </Card>
      )}

      {detail && (
      <Card className="overflow-x-auto">
        {rows.length === 0 ? (
          <Empty what="open positions" />
        ) : (
          <table className="w-full min-w-[980px]">
            <thead>
              <tr>
                <Th align="left">Account</Th>
                <Th align="left">Symbol</Th>
                <Th align="left">Side</Th>
                <Th>Qty</Th>
                <Th help="What it cost">Avg</Th>
                <Th>LTP</Th>
                <Th help="Move from the average, in the position's own direction">
                  Move
                </Th>
                <Th>Unrealised</Th>
                <Th align="right">Age</Th>
              </tr>
            </thead>
            <tbody>
              {rows.map((position) => (
                <Row
                  key={`${position.account}-${position.symbol}-${position.product_type}`}
                  position={position}
                />
              ))}
            </tbody>
          </table>
        )}
      </Card>
      )}

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        {detail
          ? "Ordered by how far each position has moved, largest first. "
          : "One row per symbol, one column per account — a name held in three places is one line. Hover a symbol for its quantities. An empty cell is \u00b7; zero is a real value and looks different. "} “Delivery” is stock
        sold out of holdings and awaiting settlement — a negative quantity, but not a
        short anyone has to buy back. Move is signed in the position’s own direction, so
        a short that has fallen shows a gain.
      </p>
    </>
  );
}
