import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader, Stat } from "../components/ui";
import { api } from "../lib/api";
import { compact, count, money, num, percent, pnlClass, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { Portfolio, PortfolioRow } from "../lib/types";

const REFRESH_MS = 5000;

function Header({ children, help }: { children: React.ReactNode; help?: string }) {
  return (
    <th
      title={help}
      className="whitespace-nowrap px-3 py-2 text-right text-xs font-semibold uppercase tracking-wide text-[var(--ink-muted)] first:text-left"
    >
      {children}
    </th>
  );
}

function Cell({
  children,
  align = "right",
  muted,
  tone,
}: {
  children: React.ReactNode;
  align?: "left" | "right";
  muted?: boolean;
  tone?: string;
}) {
  return (
    <td
      className={`tnum whitespace-nowrap px-3 py-2 text-sm ${
        align === "right" ? "text-right" : "text-left"
      } ${muted ? "text-[var(--ink-muted)]" : ""} ${tone ?? ""}`}
    >
      {children}
    </td>
  );
}

function AccountRow({ row }: { row: PortfolioRow }) {
  const pnl = num(row.pnl);
  const ret = num(row.return_pct);

  return (
    <tr
      className="border-t"
      style={{ borderColor: "var(--hairline)", opacity: row.from_store ? 0.62 : 1 }}
    >
      <Cell align="left">
        <span className="font-medium">{row.account}</span>
        {row.from_store && (
          <span className="ml-2 text-xs text-[var(--ink-muted)]">last known</span>
        )}
      </Cell>
      <Cell>
        <Money>{money(num(row.capital_in))}</Money>
      </Cell>
      <Cell>
        <Money>{money(num(row.free))}</Money>
      </Cell>
      <Cell muted>
        <Money>{money(num(row.deployed))}</Money>
      </Cell>
      <Cell>
        <Money>{money(num(row.market_value))}</Money>
      </Cell>
      <Cell tone={pnlClass(num(row.realised))}>
        <Money>{signed(num(row.realised))}</Money>
      </Cell>
      <Cell tone={pnlClass(num(row.unrealised))}>
        <Money>{signed(num(row.unrealised))}</Money>
      </Cell>
      <Cell tone={pnlClass(pnl)}>
        <strong>
          <Money>{signed(pnl)}</Money>
        </strong>
      </Cell>
      <Cell tone={pnlClass(ret)}>
        {percent(ret)}
        {row.deployed_exceeds_capital && (
          <span
            className="ml-1 text-[var(--status-warning)]"
            title="Deployed exceeds capital in — leverage, or a base missing the securities held at the year's start"
          >
            !
          </span>
        )}
      </Cell>
      <Cell>
        <span className="text-xs text-[var(--ink-muted)]">
          {count(row.counts.positions)} pos
          {row.counts.short > 0 && ` · ${row.counts.short}S`}
          {row.counts.holdings > 0 && ` · ${count(row.counts.holdings)} hld`}
        </span>
      </Cell>
    </tr>
  );
}

export function PortfolioPage() {
  const { hidden, toggle } = usePrivacy();
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["portfolio"],
    queryFn: () => api.get<Portfolio>("/portfolio"),
    refetchInterval: REFRESH_MS,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="portfolio" />;
  if (!data.totals) return <ErrorNote error="No accounts configured." />;

  const t = data.totals;
  const pnl = num(t.pnl);
  const ret = num(t.return_pct);
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

      {/* A return computed against a base nobody imported would be wildly wrong
          and look precise. Say so rather than showing it. */}
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
          note={<><Money>{compact(num(t.free))}</Money> free · <Money>{compact(num(t.deployed))}</Money> deployed at cost</>}
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

      <Card className="overflow-x-auto">
        <table className="w-full min-w-[1050px]">
          <thead>
            <tr>
              <Header>Account</Header>
              <Header help="Opening balance plus net transfers in">Capital in</Header>
              <Header help="Cash available to trade now">Free</Header>
              <Header help="Cost of what is open — what was paid, not what it is worth">
                Deployed
              </Header>
              <Header help="The same book at today's marks">Market value</Header>
              <Header help="Broker's own realised P&L for the year, net of charges">
                Realised
              </Header>
              <Header help="Mark less cost on what is still open">Unrealised</Header>
              <Header>P&amp;L</Header>
              <Header help="P&L over capital in. Unreliable where deployed exceeds capital.">
                Return
              </Header>
              <Header>Open</Header>
            </tr>
          </thead>
          <tbody>
            {data.accounts.map((row) => (
              <AccountRow key={row.account} row={row} />
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t-2" style={{ borderColor: "var(--border)" }}>
              <Cell align="left">
                <strong>All accounts</strong>
              </Cell>
              <Cell><strong><Money>{money(num(t.capital_in))}</Money></strong></Cell>
              <Cell><strong><Money>{money(num(t.free))}</Money></strong></Cell>
              <Cell muted><Money>{money(num(t.deployed))}</Money></Cell>
              <Cell><strong><Money>{money(num(t.market_value))}</Money></strong></Cell>
              <Cell tone={pnlClass(num(t.realised))}>
                <strong><Money>{signed(num(t.realised))}</Money></strong>
              </Cell>
              <Cell tone={pnlClass(num(t.unrealised))}>
                <strong><Money>{signed(num(t.unrealised))}</Money></strong>
              </Cell>
              <Cell tone={pnlClass(pnl)}>
                <strong><Money>{signed(pnl)}</Money></strong>
              </Cell>
              <Cell tone={pnlClass(ret)}>
                <strong>{percent(ret)}</strong>
              </Cell>
              <Cell>
                <span className="text-xs text-[var(--ink-muted)]">
                  {count(t.counts.positions)} pos · {count(t.counts.holdings)} hld
                </span>
              </Cell>
            </tr>
          </tfoot>
        </table>
      </Card>

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Deployed is cost, not market value — the gap between the two columns is the
        unrealised move. A short deploys margin rather than capital, so its notional is
        excluded from deployed and reported as exposure
        {num(t.short_exposure) ? <> (<Money>{money(num(t.short_exposure))}</Money> currently)</> : null}.
        Realised is the broker's own year-to-date figure net of charges.
      </p>
    </>
  );
}
