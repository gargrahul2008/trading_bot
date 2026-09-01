import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Empty, Tag, Td, Th } from "../components/DataTable";
import { api } from "../lib/api";
import { money, qty as fmtQty } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { Order, OrdersPayload } from "../lib/types";

const REFRESH_MS = 4000;

/** Rejects are the orders worth reading, so they get a named cause rather than
 *  the broker's raw text. The taxonomy is the one the live bots act on. */
const REJECT_LABEL: Record<string, string> = {
  MARGIN_SHORTFALL: "margin short",
  CIRCUIT_LIMIT: "circuit limit",
  SESSION_CLOSED: "market closed",
  DQ_NOT_ALLOWED: "no iceberg",
  AUTH_REQUIRED: "TPIN needed",
  REDUCE_QTY: "qty too high",
  NOT_RETRYABLE: "rejected",
};

const STATUS_TONE: Record<string, string> = {
  FILLED: "text-[var(--gain)]",
  REJECTED: "text-[var(--loss)]",
  CANCELLED: "text-[var(--ink-muted)]",
};

function Row({ order }: { order: Order }) {
  const bot = order.source === "bot";
  return (
    <tr className="border-t" style={{ borderColor: "var(--hairline)" }}>
      <Td align="left" muted>
        {order.trading_day}
      </Td>
      <Td align="left" muted>
        {order.account}
      </Td>
      <Td align="left">
        <span className="font-medium">{order.symbol}</span>
        <Tag title={order.product_type}>{order.product_type}</Tag>
      </Td>
      <Td align="left">
        <span
          className={`text-xs font-semibold ${
            order.side === "BUY" ? "text-[var(--gain)]" : "text-[var(--loss)]"
          }`}
        >
          {order.side}
        </span>
      </Td>
      <Td>
        {fmtQty(order.filled_qty)}
        <span className="text-[var(--ink-muted)]">/{fmtQty(order.qty)}</span>
      </Td>
      <Td muted>
        <Money>{money(order.limit_price || null)}</Money>
      </Td>
      <Td align="left">
        <span className={`text-xs font-semibold ${STATUS_TONE[order.status] ?? ""}`}>
          {order.status}
        </span>
        {order.kind && order.status === "REJECTED" && (
          <span
            className="ml-1.5 text-xs text-[var(--loss)]"
            title={order.reason ?? order.message ?? undefined}
          >
            {REJECT_LABEL[order.kind] ?? order.kind.toLowerCase()}
          </span>
        )}
      </Td>
      <Td align="left">
        {bot ? (
          <span
            className="text-xs"
            title={
              order.matched_by === "symbol"
                ? "Inferred from the run's configured symbol, not claimed by order id"
                : "Claimed by the run's own records"
            }
          >
            {order.run}
            {order.matched_by === "symbol" && "?"}
          </span>
        ) : (
          <span className="text-xs text-[var(--ink-muted)]">
            {order.source ?? "—"}
            {order.order_tag ? ` · ${order.order_tag}` : ""}
          </span>
        )}
      </Td>
    </tr>
  );
}

export function OrdersPage() {
  const { hidden, toggle } = usePrivacy();
  const [filter, setFilter] = useState<"all" | "open" | "rejected" | "bot">("all");
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["orders"],
    queryFn: () => api.get<OrdersPayload>("/orders"),
    refetchInterval: REFRESH_MS,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="orders" />;

  const all = data.orders;
  const shown = all.filter((o) =>
    filter === "all" ? true
      : filter === "open" ? o.is_open
      : filter === "rejected" ? o.status === "REJECTED"
      : o.source === "bot");

  const open = all.filter((o) => o.is_open).length;
  const rejected = all.filter((o) => o.status === "REJECTED").length;
  const bots = all.filter((o) => o.source === "bot").length;

  return (
    <>
      <PageHeader
        title="Orders"
        subtitle={
          <>
            {all.length} orders · {open} working · {bots} placed by bots
            {rejected > 0 && (
              <>
                {" · "}
                <span className="text-[var(--loss)]">{rejected} rejected</span>
              </>
            )}
          </>
        }
        actions={
          <div className="flex gap-2">
            <div className="flex rounded border" style={{ borderColor: "var(--border)" }}>
              {(["all", "open", "rejected", "bot"] as const).map((option) => (
                <button
                  key={option}
                  onClick={() => setFilter(option)}
                  className={`px-3 py-1.5 text-sm capitalize ${
                    filter === option
                      ? "bg-black/5 font-medium dark:bg-white/10"
                      : "text-[var(--ink-secondary)]"
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

      <Card className="overflow-x-auto">
        {shown.length === 0 ? (
          <Empty what={filter === "all" ? "orders yet" : `${filter} orders`} />
        ) : (
          <table className="w-full min-w-[980px]">
            <thead>
              <tr>
                <Th align="left">Day</Th>
                <Th align="left">Account</Th>
                <Th align="left">Symbol</Th>
                <Th align="left">Side</Th>
                <Th>Filled / qty</Th>
                <Th>Price</Th>
                <Th align="left">Status</Th>
                <Th align="left" help="Who placed it — the broker stamps the channel">
                  Placed by
                </Th>
              </tr>
            </thead>
            <tbody>
              {shown.map((order) => (
                <Row key={`${order.account}-${order.order_id}`} order={order} />
              ))}
            </tbody>
          </table>
        )}
      </Card>

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Today’s orders come from the agents and update within seconds; earlier days come
        from the store. A reject shows its cause — margin, circuit, session, TPIN — from
        the same parser the live bots act on. A run name with “?” was inferred from the
        run’s configured symbol rather than claimed by order id.
      </p>
    </>
  );
}
