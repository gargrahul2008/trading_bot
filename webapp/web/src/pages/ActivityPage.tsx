import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Empty } from "../components/DataTable";
import { api } from "../lib/api";
import { money, num, qty as fmtQty, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { ActivityEvent, ActivityPayload } from "../lib/types";

const REFRESH_MS = 4000;

/** A word for each thing that can happen, and the colour it deserves. Closes
 *  and rejects are what someone scans for; the rest are context around them. */
const EVENT: Record<string, { label: string; tone: string; dot: string }> = {
  placed: { label: "placed", tone: "text-[var(--ink-secondary)]", dot: "var(--ink-muted)" },
  partial: { label: "part filled", tone: "text-[var(--ink-secondary)]", dot: "var(--ink-muted)" },
  filled: { label: "filled", tone: "text-[var(--ink)]", dot: "var(--ink)" },
  cancelled: { label: "cancelled", tone: "text-[var(--ink-muted)]", dot: "var(--ink-muted)" },
  rejected: { label: "rejected", tone: "text-[var(--loss)]", dot: "var(--loss)" },
  changed: { label: "changed", tone: "text-[var(--ink-secondary)]", dot: "var(--ink-muted)" },
  closed: { label: "closed", tone: "text-[var(--ink)]", dot: "var(--accent, var(--ink))" },
};

/** Times arrive as epoch seconds from the event log and as broker strings from
 *  a matched trade. Both are shown as a clock reading, never re-zoned: the
 *  figures are IST because the market is. */
function clock(at: ActivityEvent["at"]): string {
  if (typeof at === "number") {
    return new Date(at * 1000).toLocaleTimeString("en-IN", {
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    });
  }
  if (typeof at === "string" && at.length >= 16) return at.slice(11, 19) || "—";
  return "—";
}

function Line({ event }: { event: ActivityEvent }) {
  const meta = EVENT[event.event] ?? EVENT.changed;
  // Net when the day's charges are known, gross when they are not — an unknown
  // cost is not a zero one, and showing nothing would hide the trade's result.
  const net = event.net_pnl == null ? null : num(event.net_pnl);
  const pnl = net ?? (event.pnl == null ? null : num(event.pnl));
  const closed = event.event === "closed";

  return (
    <li className="flex gap-3 border-t px-4 py-2.5" style={{ borderColor: "var(--hairline)" }}>
      <span className="w-16 shrink-0 pt-0.5 font-mono text-xs text-[var(--ink-muted)]">
        {clock(event.at)}
      </span>
      <span
        className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full"
        style={{ background: meta.dot }}
      />
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-baseline gap-x-2">
          <span className={`text-sm font-semibold ${meta.tone}`}>{meta.label}</span>
          <span className="text-sm font-medium">{event.symbol}</span>
          <span
            className={`text-xs font-semibold ${
              event.side === "BUY" ? "text-[var(--gain)]" : "text-[var(--loss)]"
            }`}
          >
            {event.side}
          </span>
          {event.qty ? (
            <span className="text-xs text-[var(--ink-secondary)]">
              {fmtQty(num(event.event === "partial" ? event.filled_qty : event.qty))}
              {event.event === "partial" && <>/{fmtQty(num(event.qty))}</>}
            </span>
          ) : null}
          {event.price ? (
            <span className="text-xs text-[var(--ink-muted)]">
              <Money>{money(num(event.price))}</Money>
            </span>
          ) : null}
          <span className="ml-auto text-xs text-[var(--ink-muted)]">{event.account}</span>
        </div>
        <div className="mt-0.5 flex flex-wrap items-baseline gap-x-2 text-xs text-[var(--ink-muted)]">
          {closed && event.entry_price ? (
            <span>
              in <Money>{money(num(event.entry_price))}</Money> → out{" "}
              <Money>{money(num(event.price))}</Money>
            </span>
          ) : null}
          {closed && pnl != null ? (
            <span
              className={`font-semibold ${
                pnl >= 0 ? "text-[var(--gain)]" : "text-[var(--loss)]"
              }`}
            >
              <Money>{signed(pnl)}</Money> {net === null ? "gross" : "net"}
              {net === null ? (
                <span className="font-normal text-[var(--ink-muted)]">
                  {" "}
                  (charges for the day not yet in)
                </span>
              ) : event.charges ? (
                <span className="font-normal text-[var(--ink-muted)]">
                  {" "}
                  (after <Money>{money(num(event.charges))}</Money> charges)
                </span>
              ) : null}
            </span>
          ) : null}
          {event.event === "rejected" && (
            <span className="text-[var(--loss)]">{event.reason ?? event.message}</span>
          )}
          {event.source === "bot" ? (
            <span>by {event.run ?? "bot"}</span>
          ) : event.source ? (
            <span>{event.source}</span>
          ) : null}
        </div>
      </div>
    </li>
  );
}

export function ActivityPage() {
  const { hidden, toggle } = usePrivacy();
  const [filter, setFilter] = useState<"all" | "closed" | "rejected" | "manual">("all");
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["activity"],
    queryFn: () => api.get<ActivityPayload>("/activity"),
    refetchInterval: REFRESH_MS,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="activity" />;

  const all = data.events;
  const shown = all.filter((e) =>
    filter === "all" ? true
      : filter === "closed" ? e.event === "closed"
      : filter === "rejected" ? e.event === "rejected"
      : e.source !== "bot");

  const closed = all.filter((e) => e.event === "closed");
  const realised = closed.reduce(
    (sum, e) => sum + (num(e.net_pnl ?? e.pnl ?? null) ?? 0), 0);

  // Grouped by trading day so a scan reads "today, then yesterday" rather than
  // a single undifferentiated run of times.
  const days: string[] = [];
  for (const event of shown) {
    if (event.trading_day && days[days.length - 1] !== event.trading_day) {
      if (!days.includes(event.trading_day)) days.push(event.trading_day);
    }
  }

  return (
    <>
      <PageHeader
        title="Activity"
        subtitle={
          <>
            {all.length} events · {closed.length} positions closed
            {closed.length > 0 && (
              <>
                {" · "}
                <span className={realised >= 0 ? "text-[var(--gain)]" : "text-[var(--loss)]"}>
                  <Money>{signed(realised)}</Money> realised
                </span>
              </>
            )}
          </>
        }
        actions={
          <div className="flex gap-2">
            <div className="flex rounded border" style={{ borderColor: "var(--border)" }}>
              {(["all", "closed", "rejected", "manual"] as const).map((option) => (
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

      {shown.length === 0 ? (
        <Card>
          <Empty what={filter === "all" ? "activity yet" : `${filter} events`} />
        </Card>
      ) : (
        days.map((day) => (
          <Card key={day} className="mb-3 overflow-hidden">
            <div
              className="border-b px-4 py-2 text-xs font-semibold uppercase tracking-wide text-[var(--ink-secondary)]"
              style={{ borderColor: "var(--hairline)" }}
            >
              {day}
            </div>
            <ul>
              {shown
                .filter((e) => e.trading_day === day)
                .map((event, index) => (
                  <Line key={`${event.account}-${event.order_id}-${event.event}-${index}`} event={event} />
                ))}
            </ul>
          </Card>
        ))
      )}

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Every change, newest first. An order’s status change is recorded as it happens, so a
        fill or a reject is still here after the order itself has moved on. A close is
        derived from matching the exit against what opened the position, which is why it
        carries an entry price and a net figure the broker does not report.
      </p>
    </>
  );
}
