import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader, Stat } from "../components/ui";
import { Empty, Th } from "../components/DataTable";
import { AlertToasts, useAlertEvents } from "../components/AlertToasts";
import type { AlertEvent } from "../components/AlertToasts";
import { api } from "../lib/api";
import { money, num, pnlClass } from "../lib/format";
import { Money } from "../lib/privacy";
import type { DashboardLine, DashboardPayload } from "../lib/types";

/** One page for the whole book: what it cost, what it is worth, what moved.
 *
 *  Three blocks, in the order the questions get asked. The totals, because that
 *  is the state of things. Then what moved past a threshold you set, because
 *  that is what needs a decision. Then the extremes by money, because that is
 *  where the account is actually won or lost.
 *
 *  Two percentages appear throughout and they are not interchangeable:
 *
 *  * **Today** is against yesterday's close. It answers "what is happening now",
 *    and it costs a broker call per symbol — nothing in a positions or holdings
 *    payload carries a day change, so it is fetched separately and cached for a
 *    minute server-side.
 *  * **On cost** is against the average paid. It answers "how is this position
 *    doing", and it is free, because both numbers are already held.
 *
 *  A position can be up 5% today and down 87% on cost. Showing one of them
 *  alone is how a portfolio looks fine on a day it happens to bounce.
 */

/** Thresholds worth having one press away. Typed over freely. */
const PRESETS = [2, 3, 5, 10];
const STORAGE_KEY = "dashboard.threshold";

/** `NSE:LEMONTREE-EQ` -> `LEMONTREE`.
 *
 *  The exchange and the series are the same on nearly every row and carry no
 *  information at a glance; the name is what is being read. Kept in the row's
 *  title so the full symbol is still one hover away.
 */
function shortName(symbol: string): string {
  return symbol.replace(/^[A-Z]+:/, "").replace(/-[A-Z0-9]{1,3}$/, "");
}

/** An alert row: what it is, where it moved from, and by how much.
 *
 *  A flex line rather than a table because an alert has one figure worth
 *  aligning. The movers box next to it is a real table — three numbers per row
 *  that have to line up with each other — and the difference is deliberate.
 *
 *  `detail` is deliberately lighter and a size down: it is the evidence, not the
 *  headline. `headline` is bold and coloured, and sits right. `trailing` is kept
 *  for a second supporting figure after the headline.
 */
function CompactRow({
  name,
  detail,
  account,
  headline,
  trailing,
  tone,
  title,
}: {
  name: string;
  detail: ReactNode;
  account: string;
  headline: ReactNode;
  trailing?: ReactNode;
  tone?: string;
  title?: string;
}) {
  return (
    <li
      className="flex items-baseline gap-2 border-t px-4 py-2 first:border-t-0"
      style={{ borderColor: "var(--hairline)" }}
      title={title}
    >
      <span className="text-sm font-medium">{name}</span>
      <span className="text-[10px] uppercase tracking-wide text-[var(--ink-muted)]">
        {account}
      </span>
      {/* Size and alignment only — the colour belongs to the caller. An alert's
          price pair is muted because it is evidence; a mover's percentages are
          tinted by sign because the sign is the information. */}
      <span className="ml-auto flex items-baseline gap-2">
        <span className="tnum text-xs">{detail}</span>
        <span className={`tnum text-sm font-semibold ${tone ?? ""}`}>{headline}</span>
        {trailing !== undefined && <span className="tnum text-xs">{trailing}</span>}
      </span>
    </li>
  );
}

/** Today's move: where it went, and from where. */
function AlertRow({ line }: { line: DashboardLine }) {
  const pct = line.day_change_pct;
  const prev = line.day_change === null || pct === null ? null : line.ltp - line.day_change;

  return (
    <CompactRow
      name={shortName(line.symbol)}
      detail={
        <span className="text-[var(--ink-muted)]">
          {money(line.ltp)}
          {prev !== null && <> from {money(prev)}</>}
        </span>
      }
      account={line.account}
      tone={pnlClass(pct ?? 0)}
      headline={
        pct === null ? "—" : `${pct > 0 ? "\u25b2" : "\u25bc"} ${Math.abs(pct).toFixed(2)}%`
      }
      title={`${line.symbol} · ${line.account}`}
    />
  );
}

/** A percentage, tinted by its own sign.
 *
 *  Each figure on a mover row is coloured independently, so a row can read
 *  red-green-green: WHEELS is down today, up on the money, up on cost. Colouring
 *  the row as a whole, or leaving them all muted, hid exactly that.
 *
 *  Null is an em dash, never 0.00% — a flat day and an unpriced symbol are
 *  different answers.
 */
function MoverPct({ value }: { value: number | null }) {
  if (value === null) {
    return (
      <span className="text-[var(--ink-muted)]" title="The broker would not price this symbol">
        —
      </span>
    );
  }
  return (
    <span className={pnlClass(value)}>
      {value > 0 ? "+" : ""}
      {value.toFixed(2)}%
    </span>
  );
}

/** A mover row: today's move, the money, the move on cost.
 *
 *  Ranked by the money, so the money is bold and the percentages are not.
 */
function MoverRow({ line }: { line: DashboardLine }) {
  return (
    <tr className="border-t" style={{ borderColor: "var(--hairline)" }} title={line.symbol}>
      <td className="px-4 py-2 text-sm font-medium">{shortName(line.symbol)}</td>
      <td className="px-2 py-2 text-[10px] uppercase tracking-wide text-[var(--ink-muted)]">
        {line.account}
      </td>
      <td className="tnum px-2 py-2 text-right text-xs">
        <MoverPct value={line.day_change_pct} />
      </td>
      <td
        className={`tnum px-2 py-2 text-right text-sm font-semibold ${pnlClass(line.unrealised)}`}
      >
        <Money>{money(line.unrealised)}</Money>
      </td>
      <td className="tnum px-4 py-2 text-right text-xs">
        <MoverPct value={line.unrealised_pct} />
      </td>
    </tr>
  );
}

/** Both halves of the movers box, as one table.
 *
 *  Gainers above, losers below, in one card — they are read as a pair, and a
 *  single surround says so. One table rather than two so the three numeric
 *  columns share their widths across both halves: a loser's percentage sits
 *  directly under a gainer's, which is the whole point of giving them columns.
 *
 *  Each half keeps a heading row inside the table, because without it the two
 *  blocks merge into one ranked list that reads as nonsense at the join — the
 *  smallest gain sitting directly above the smallest loss.
 */
function MoverHeading({ title, empty }: { title: string; empty: boolean }) {
  return (
    <tr className="border-t" style={{ borderColor: "var(--border)" }}>
      <td
        colSpan={5}
        className="bg-black/[0.02] px-4 py-1 text-[10px] font-semibold uppercase tracking-wide text-[var(--ink-muted)] dark:bg-white/[0.03]"
      >
        {title}
        {empty && <span className="ml-1.5 font-normal normal-case">— none</span>}
      </td>
    </tr>
  );
}

function MoversTable({
  gainers,
  losers,
}: {
  gainers: DashboardLine[];
  losers: DashboardLine[];
}) {
  return (
    <table className="w-full">
      <thead>
        <tr>
          <Th align="left">Symbol</Th>
          <Th align="left">Account</Th>
          <Th help="Against yesterday's close">Today</Th>
          <Th help="Unrealised, at today's mark — what the ranking is on">P&amp;L</Th>
          <Th help="Against the average paid">On cost</Th>
        </tr>
      </thead>
      <tbody>
        <MoverHeading title="Top gainers" empty={gainers.length === 0} />
        {gainers.map((line) => (
          <MoverRow key={`g-${line.account}-${line.symbol}`} line={line} />
        ))}
        <MoverHeading title="Top losers" empty={losers.length === 0} />
        {losers.map((line) => (
          <MoverRow key={`l-${line.account}-${line.symbol}`} line={line} />
        ))}
      </tbody>
    </table>
  );
}

export function DashboardPage() {
  // Kept across reloads: the threshold is a working preference, and retyping it
  // every visit is how a control stops being used.
  const [threshold, setThreshold] = useState(() => {
    const saved = window.localStorage.getItem(STORAGE_KEY);
    const parsed = saved === null ? NaN : Number(saved);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 3;
  });
  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, String(threshold));
  }, [threshold]);

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["dashboard", threshold],
    queryFn: () =>
      api.get<DashboardPayload>(`/dashboard?threshold=${encodeURIComponent(threshold)}`),
    // Five minutes, matching the server's day-change cache. Polling faster only
    // re-renders the same cached figures and would make the alert sound fire on
    // a schedule the prices are not actually moving on.
    refetchInterval: 5 * 60_000,
  });

  // Only symbols that have just crossed the threshold, never the ones already
  // over it — see AlertToasts for why that distinction is the whole feature.
  const alertEvents: AlertEvent[] | undefined = data?.alerts.map((line) => ({
    key: `${line.account}-${line.symbol}`,
    symbol: shortName(line.symbol),
    account: line.account,
    pct: line.day_change_pct ?? 0,
    ltp: line.ltp,
    prev: line.day_change === null ? null : line.ltp - line.day_change,
  }));
  const { events, dismiss, muted, setMuted, test } = useAlertEvents(
    alertEvents,
    Boolean(data),
  );

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="the book" />;

  const t = data.totals;
  const invested = num(t.invested) ?? 0;
  const unrealised = num(t.unrealised) ?? 0;

  return (
    <>
      <AlertToasts events={events} onDismiss={dismiss} />
      <PageHeader
        title="Dashboard"
        subtitle={`${t.positions} open positions across ${data.accounts.length} accounts`}
      />

      {data.accounts_missing.length > 0 && (
        <p className="mb-3 text-xs text-[var(--warning)]">
          {data.accounts_missing.join(", ")} did not answer — figures below exclude{" "}
          {data.accounts_missing.length === 1 ? "it" : "them"}.
        </p>
      )}

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat
          label="Total investment"
          value={<Money>{money(invested)}</Money>}
          note="What was paid for everything still open"
        />
        <Stat
          label="Current value"
          value={<Money>{money(num(t.market_value) ?? 0)}</Money>}
          note="The same book at today's marks"
        />
        <Stat
          label="Unrealised"
          value={<Money>{money(unrealised)}</Money>}
          tone={pnlClass(unrealised)}
          note={
            invested > 0 ? `${((unrealised / invested) * 100).toFixed(2)}% on cost` : undefined
          }
        />
        <Stat
          label="Realised"
          value={
            t.realised_available ? (
              <Money>{money(num(t.realised) ?? 0)}</Money>
            ) : (
              <span className="text-[var(--ink-muted)]">—</span>
            )
          }
          tone={t.realised_available ? pnlClass(num(t.realised) ?? 0) : undefined}
          note="Year to date, net of charges"
        />
      </div>

      {/* Alerts left, the movers pair right, equal width. The movers tables
          became compact rows too, so neither side needs more room than the
          other. Stacked on a small screen, alerts first — the thing that needs
          a decision leads. */}
      <div className="mt-4 grid gap-3 lg:grid-cols-2">
        {/* ── price alerts ─────────────────────────────────────────────── */}
        <Card>
          <div
            className="flex flex-wrap items-baseline gap-x-2 gap-y-1 border-b px-4 py-2.5"
            style={{ borderColor: "var(--hairline)" }}
          >
            <span className="text-sm font-semibold">Price alerts</span>
            <span className="text-xs text-[var(--ink-muted)]">moves over {threshold}%</span>
            <span className="ml-auto flex items-center gap-1">
              {/* Doubles as the gesture browsers require before they will play
                  audio at all, which is why unmuting also plays a test tone. */}
              <button
                type="button"
                onClick={() => {
                  const next = !muted;
                  setMuted(next);
                  if (!next) test();
                }}
                title={muted ? "Alert sound off — click to enable" : "Alert sound on"}
                aria-label={muted ? "Enable alert sound" : "Mute alert sound"}
                className="rounded px-1.5 py-0.5 text-xs text-[var(--ink-secondary)] transition hover:bg-black/5 dark:hover:bg-white/10"
              >
                {muted ? "\u{1f507}" : "\u{1f50a}"}
              </button>
              {PRESETS.map((preset) => (
                <button
                  key={preset}
                  type="button"
                  onClick={() => setThreshold(preset)}
                  className={`rounded px-1.5 py-0.5 text-xs font-medium transition ${
                    threshold === preset
                      ? "bg-black/10 dark:bg-white/15"
                      : "text-[var(--ink-secondary)] hover:bg-black/5 dark:hover:bg-white/10"
                  }`}
                >
                  {preset}%
                </button>
              ))}
              <input
                value={threshold}
                onChange={(event) => {
                  const next = Number(event.target.value);
                  if (Number.isFinite(next) && next >= 0) setThreshold(next);
                }}
                inputMode="decimal"
                aria-label="Alert threshold, percent"
                className="tnum w-12 rounded border bg-transparent px-1.5 py-0.5 text-xs"
                style={{ borderColor: "var(--border)" }}
              />
            </span>
          </div>

          {!data.day_change_available && (
            <p className="px-4 py-2 text-xs text-[var(--ink-muted)]">
              No day change available — the agents did not price these symbols.
            </p>
          )}

          {data.alerts.length === 0 ? (
            <Empty what={`moves over ${threshold}%`} />
          ) : (
            <ul>
              {data.alerts.map((line) => (
                <AlertRow key={`${line.account}-${line.symbol}`} line={line} />
              ))}
            </ul>
          )}
        </Card>

        {/* ── top movers, gainers above losers in one box ──────────────── */}
        <Card className="overflow-x-auto">
          <div
            className="flex items-baseline gap-2 border-b px-4 py-2.5"
            style={{ borderColor: "var(--hairline)" }}
          >
            <span className="text-sm font-semibold">Top movers</span>
            <span className="text-xs text-[var(--ink-muted)]">
              the {data.gainers.length} best and {data.losers.length} worst by money
            </span>
          </div>
          <MoversTable gainers={data.gainers} losers={data.losers} />
        </Card>
      </div>

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Alerts measure <strong>yesterday's close to the last traded price</strong> — today's
        move, nothing else. On-cost was tried as a second trigger and removed: a holding 87%
        down on cost that has not moved since yesterday is not news, and it buried the symbols
        that actually moved. On-cost still appears in the movers tables, where the question is
        how a position is doing rather than what just happened. Those rank by money rather than
        percent, because a small move on a large position outweighs a large move on a small one;
        the percentages are shown so the reverse case stays visible. Day change costs a broker
        call per symbol and is cached for a minute, so it can lag the mark by that much.
      </p>
    </>
  );
}
