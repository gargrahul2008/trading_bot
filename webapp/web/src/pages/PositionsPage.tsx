import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Empty, Tag, Td, Th } from "../components/DataTable";
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

export function PositionsPage() {
  const { hidden, toggle } = usePrivacy();
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["positions"],
    queryFn: () => api.get<PositionsPayload>("/positions"),
    refetchInterval: REFRESH_MS,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="positions" />;

  const rows = data.positions;
  const total = rows.reduce((sum, p) => sum + p.unrealised, 0);
  const longs = rows.filter((p) => p.net_qty > 0 && !p.delivery_sale).length;
  const shorts = rows.filter((p) => p.net_qty < 0 && !p.delivery_sale).length;
  const sold = rows.filter((p) => p.delivery_sale).length;

  return (
    <>
      <PageHeader
        title="Positions"
        subtitle={
          <>
            {rows.length} open · {longs} long · {shorts} short
            {sold > 0 && ` · ${sold} sold from holdings`} · unrealised{" "}
            <span className={pnlClass(total)}>{signed(total)}</span>
          </>
        }
        actions={
          <button
            onClick={toggle}
            className="rounded border px-3 py-1.5 text-sm"
            style={{ borderColor: "var(--border)" }}
          >
            {hidden ? "Show figures" : "Hide figures"}
          </button>
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

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Ordered by how far each position has moved, largest first. “Delivery” is stock
        sold out of holdings and awaiting settlement — a negative quantity, but not a
        short anyone has to buy back. Move is signed in the position’s own direction, so
        a short that has fallen shows a gain.
      </p>
    </>
  );
}
