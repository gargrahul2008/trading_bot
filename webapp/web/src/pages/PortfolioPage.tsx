import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader, Stat } from "../components/ui";
import { AccountStatus, Chip } from "../components/StatusChip";
import { RiskLimits } from "../components/RiskLimits";
import { api } from "../lib/api";
import { age, compact, count, money, num, percent, pnlClass, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { AccountRow, Overview, Portfolio, PortfolioRow } from "../lib/types";

/** The portfolio, one account at a time and then as one book.
 *
 *  Previously two pages: Portfolio held the year's capital and P&L, Accounts
 *  held today's cash and reachability. They describe the same accounts, and
 *  answering "what is in pratibha right now" meant reading both and joining
 *  them by eye. One page per account, headed by the account.
 *
 *  The two sources stay separate behind it: /portfolio is computed from the
 *  year's ledger and matched history, /overview is what the agent last saw.
 *  They refresh at different rates because they change at different rates.
 */
const SLOW_MS = 5000;   // capital and year-to-date P&L
const FAST_MS = 3000;   // cash, reachability, today's orders

function Figure({
  label,
  value,
  note,
  tone,
  help,
}: {
  label: string;
  value: React.ReactNode;
  note?: React.ReactNode;
  tone?: string;
  help?: string;
}) {
  return (
    <div title={help}>
      <div className="text-[10px] uppercase tracking-wide text-[var(--ink-muted)]">
        {label}
      </div>
      <div className={`tnum mt-0.5 text-sm font-semibold ${tone ?? ""}`}>{value}</div>
      {note && <div className="text-[11px] text-[var(--ink-muted)]">{note}</div>}
    </div>
  );
}

function AccountCard({ row, live }: { row: PortfolioRow; live?: AccountRow }) {
  const pnl = num(row.pnl);
  const ret = num(row.return_pct);
  const funds = live?.funds ?? null;
  const orders = live?.orders ?? null;

  return (
    <Card className="mb-3 overflow-hidden">
      <div
        className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b px-4 py-2.5"
        style={{
          borderColor: "var(--hairline)",
          background: "color-mix(in srgb, var(--ink) 3%, var(--surface))",
        }}
      >
        <span className="text-sm font-semibold">{row.account}</span>
        {live && <AccountStatus row={live} />}
        {live?.allow_trading && (
          <Chip tone="warning" label="trading on" title="This agent can place orders" />
        )}
        {row.from_store && !live && (
          <span className="text-xs text-[var(--ink-muted)]">last known</span>
        )}

        <span className="ml-auto flex items-baseline gap-2">
          <span className={`tnum text-sm font-semibold ${pnlClass(pnl)}`}>
            <Money>{signed(pnl)}</Money>
          </span>
          <span className={`tnum text-xs ${pnlClass(ret)}`}>
            {ret === null ? "" : `(${percent(ret)})`}
          </span>
        </span>
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-3 px-4 py-3 sm:grid-cols-4 xl:grid-cols-8">
        <Figure
          label="Capital in"
          value={<Money>{money(num(row.capital_in))}</Money>}
          help="Opening balance plus net transfers in"
          note={num(row.capital_in) === 0 ? "not recorded" : undefined}
        />
        <Figure
          label="Free"
          value={<Money>{money(funds ? funds.available : num(row.free))}</Money>}
          help="Cash available to trade now"
          note={
            funds ? <><Money>{money(funds.utilised)}</Money> utilised</> : undefined
          }
        />
        <Figure
          label="Deployed"
          value={<Money>{money(num(row.deployed))}</Money>}
          help="Cost of what is open — what was paid, not what it is worth"
          note={
            row.deployed_exceeds_capital ? (
              <span className="text-[var(--status-warning)]">over capital</span>
            ) : undefined
          }
        />
        <Figure
          label="Market value"
          value={<Money>{money(num(row.market_value))}</Money>}
          help="The same book at today's marks"
        />
        <Figure
          label="Realised YTD"
          value={<Money>{signed(num(row.realised))}</Money>}
          tone={pnlClass(num(row.realised))}
          help="Broker's own realised P&L for the year, net of charges"
          note={
            funds ? (
              <>
                today <Money>{signed(funds.realised_today)}</Money>
              </>
            ) : undefined
          }
        />
        <Figure
          label="Unrealised"
          value={<Money>{signed(num(row.unrealised))}</Money>}
          tone={pnlClass(num(row.unrealised))}
          help="Mark less cost on what is still open"
        />
        <Figure
          label="Open"
          value={
            <>
              {count(row.counts.positions)}
              <span className="ml-1 text-xs font-normal text-[var(--ink-muted)]">
                pos
              </span>
            </>
          }
          note={
            <>
              {row.counts.short > 0 && `${row.counts.short} short · `}
              {count(row.counts.holdings)} holdings
            </>
          }
        />
        <Figure
          label="Orders today"
          value={
            orders ? (
              <>
                {count(orders.open)}
                <span className="ml-1 text-xs font-normal text-[var(--ink-muted)]">
                  open
                </span>
              </>
            ) : (
              <span className="text-[var(--ink-muted)]">—</span>
            )
          }
          note={
            orders && orders.total > 0 ? (
              <>
                of {count(orders.total)} · {orders.by_source.bot ?? 0} bot /{" "}
                {orders.by_source.manual ?? 0} manual
                {orders.rejected > 0 && (
                  <span className="text-[var(--loss)]"> · {orders.rejected} rej</span>
                )}
              </>
            ) : undefined
          }
        />
      </div>
    </Card>
  );
}

export function PortfolioPage() {
  const { hidden, toggle } = usePrivacy();
  const book = useQuery({
    queryKey: ["portfolio"],
    queryFn: () => api.get<Portfolio>("/portfolio"),
    refetchInterval: SLOW_MS,
  });
  // Reachability and today's cash. A failure here costs the status chips and
  // nothing else — the year's figures do not depend on it.
  const overview = useQuery({
    queryKey: ["overview"],
    queryFn: () => api.get<Overview>("/overview"),
    refetchInterval: FAST_MS,
  });

  if (book.isError) return <ErrorNote error={book.error} />;
  if (book.isLoading || !book.data) return <Loading what="portfolio" />;
  if (!book.data.totals) return <ErrorNote error="No accounts configured." />;

  const data = book.data;
  const t = data.totals!;
  const pnl = num(t.pnl);
  const ret = num(t.return_pct);
  const aside = t.excluded;
  const byAccount = new Map(
    (overview.data?.accounts ?? []).map((row) => [row.account, row]),
  );
  const missing = overview.data?.totals.accounts_missing ?? [];
  const noCapital = data.accounts.filter((row) => num(row.capital_in) === 0);
  const overDeployed = data.accounts.filter((row) => row.deployed_exceeds_capital);

  return (
    <>
      <PageHeader
        title="Portfolio"
        subtitle={
          <>
            {t.accounts} accounts as one book · since {data.fy_start} ·{" "}
            {count(t.counts.positions)} open positions
            {overview.data && (
              <> · updated {age((Date.now() - overview.dataUpdatedAt) / 1000)}</>
            )}
            {aside?.count > 0 && (
              <>
                {" · "}
                <span title={aside.symbols.join(", ")}>
                  {aside.count} set aside, worth <Money>{money(num(aside.cost))}</Money> at
                  cost, excluded from every figure below
                </span>
              </>
            )}
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

      {/* A total that quietly omits an unreachable account is a wrong number
          presented as a right one. Say so, above the numbers it affects. */}
      {missing.length > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-critical)", color: "var(--ink)" }}
        >
          <strong>{missing.join(", ")}</strong> could not be reached and has nothing
          stored — its figures are missing from the totals below.
        </div>
      )}

      {/* A base too small makes every return too large. Say which of the two
          causes it could be rather than showing a confident percentage. */}
      {overDeployed.length > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-warning)", color: "var(--ink)" }}
        >
          <strong>{overDeployed.map((r) => r.account).join(", ")}</strong> deployed more
          than the capital recorded for {overDeployed.length === 1 ? "it" : "them"}. That
          is expected on a leveraged account (MTF is 3×) — otherwise the base is missing
          the securities already held on {data.fy_start}, which the broker's ledger does
          not record. Enter it with{" "}
          <code>scripts/capital.py --set &lt;account&gt; &lt;amount&gt;</code> and the
          returns become measurable.
        </div>
      )}

      {noCapital.length > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-warning)", color: "var(--ink)" }}
        >
          No capital recorded for <strong>{noCapital.map((r) => r.account).join(", ")}</strong> —
          their returns show as “—”. Import it with{" "}
          <code>scripts/fetch_history.py --from {data.fy_start}</code>.
        </div>
      )}

      <div className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Stat
          label="Capital in"
          value={<Money>{compact(num(t.capital_in))}</Money>}
          note={
            <>
              <Money>{compact(num(t.free))}</Money> free ·{" "}
              <Money>{compact(num(t.deployed))}</Money> deployed at cost
            </>
          }
        />
        <Stat
          label="Realised — year to date"
          value={<Money>{compact(num(t.realised))}</Money>}
          tone={pnlClass(num(t.realised))}
          note="broker's own figure, net of charges"
        />
        <Stat
          label="Unrealised"
          value={<Money>{compact(num(t.unrealised))}</Money>}
          tone={pnlClass(num(t.unrealised))}
          note={<><Money>{compact(num(t.market_value))}</Money> at market</>}
        />
        <Stat
          label="Total P&L"
          value={<Money>{compact(pnl)}</Money>}
          tone={pnlClass(pnl)}
          note={ret === null ? "no capital recorded" : `${percent(ret)} on capital in`}
        />
      </div>

      {data.accounts.map((row) => (
        <AccountCard key={row.account} row={row} live={byAccount.get(row.account)} />
      ))}

      <RiskLimits accounts={data.accounts.map((row) => row.account)} />

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Deployed is cost, not market value — the gap between it and market value is the
        unrealised move. A short deploys margin rather than capital, so its notional is
        excluded from deployed and reported as exposure
        {num(t.short_exposure) ? (
          <> (<Money>{money(num(t.short_exposure))}</Money> currently)</>
        ) : null}
        . Realised YTD is the broker's own year-to-date figure net of charges; “today” beside
        it is the broker's day figure, marked from the previous close. They are different
        numbers and are never added together.
      </p>
    </>
  );
}
