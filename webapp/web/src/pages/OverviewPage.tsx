import { useQuery } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader, Stat } from "../components/ui";
import { AccountStatus, Chip } from "../components/StatusChip";
import { api } from "../lib/api";
import { age, count, money, plural, pnlClass, signed } from "../lib/format";
import { Money, usePrivacy } from "../lib/privacy";
import type { AccountRow, Overview } from "../lib/types";

/** How often the browser asks the API. The agents poll the brokers on their own
 *  schedule regardless of this, so opening ten tabs costs the brokers nothing —
 *  that decoupling is what keeps the dashboard from competing with the bots. */
const REFRESH_MS = 3000;

function Header({ children }: { children: React.ReactNode }) {
  return (
    <th className="whitespace-nowrap px-3 py-2 text-left text-xs font-semibold uppercase tracking-wide text-[var(--ink-muted)]">
      {children}
    </th>
  );
}

function Cell({
  children,
  align = "right",
  muted,
}: {
  children: React.ReactNode;
  align?: "left" | "right";
  muted?: boolean;
}) {
  return (
    <td
      className={`tnum whitespace-nowrap px-3 py-2 text-sm ${
        align === "right" ? "text-right" : "text-left"
      } ${muted ? "text-[var(--ink-muted)]" : ""}`}
    >
      {children}
    </td>
  );
}

function AccountRowView({ row }: { row: AccountRow }) {
  // An unreachable account keeps its row rather than vanishing: a missing row
  // reads as "this account has nothing in it", which is a very different claim
  // from "we could not reach it".
  if (!row.reachable || !row.funds || !row.positions || !row.holdings || !row.orders) {
    return (
      <tr className="border-t" style={{ borderColor: "var(--hairline)" }}>
        <Cell align="left">
          <span className="font-medium">{row.account}</span>
        </Cell>
        <Cell align="left">
          <AccountStatus row={row} />
        </Cell>
        <td colSpan={8} className="px-3 py-2 text-sm text-[var(--ink-muted)]">
          {row.error ?? "no data"}
        </td>
      </tr>
    );
  }

  const { funds, positions, holdings, orders } = row;
  const bots = orders.by_source.bot ?? 0;
  const manual = orders.by_source.manual ?? 0;

  return (
    <tr
      className="border-t"
      style={{ borderColor: "var(--hairline)", opacity: row.from_store ? 0.62 : 1 }}
    >
      <Cell align="left">
        <span className="font-medium">{row.account}</span>
        {row.allow_trading && (
          <span className="ml-2 align-middle">
            <Chip tone="warning" label="trading on" title="This agent can place orders" />
          </span>
        )}
      </Cell>
      <Cell align="left">
        <AccountStatus row={row} />
      </Cell>
      <Cell>
        <Money>{money(funds.available)}</Money>
      </Cell>
      <Cell muted>
        <Money>{money(funds.utilised)}</Money>
      </Cell>
      <Cell>
        <span className={pnlClass(funds.realised_today)}>
          <Money>{signed(funds.realised_today)}</Money>
        </span>
      </Cell>
      <Cell>
        <span className={pnlClass(positions.unrealised)}>
          <Money>{signed(positions.unrealised)}</Money>
        </span>
      </Cell>
      <Cell align="left">
        {positions.open === 0 ? (
          <span className="text-[var(--ink-muted)]">—</span>
        ) : (
          <span className="text-sm">
            {count(positions.open)}
            <span className="ml-1 text-xs text-[var(--ink-muted)]">
              ({positions.long}L / {positions.short}S
              {positions.delivery_sales > 0 && ` / ${positions.delivery_sales} sold`})
            </span>
          </span>
        )}
      </Cell>
      <Cell>
        <Money>{money(holdings.market_value)}</Money>
      </Cell>
      <Cell>
        <span className={pnlClass(holdings.unrealised)}>
          <Money>{signed(holdings.unrealised)}</Money>
        </span>
      </Cell>
      <Cell align="left">
        <span className="text-sm">{plural(orders.open, "open order")}</span>
        <span className="ml-1 text-xs text-[var(--ink-muted)]">
          ({bots} bot / {manual} manual)
        </span>
      </Cell>
    </tr>
  );
}

export function OverviewPage() {
  const { hidden, toggle } = usePrivacy();
  const { data, isLoading, isError, error, dataUpdatedAt } = useQuery({
    queryKey: ["overview"],
    queryFn: () => api.get<Overview>("/overview"),
    refetchInterval: REFRESH_MS,
  });

  if (isError) return <ErrorNote error={error} />;
  if (isLoading || !data) return <Loading what="accounts" />;

  const { totals, accounts } = data;
  const missing = totals.accounts_missing;
  const stored = totals.accounts_from_store ?? [];

  return (
    <>
      <PageHeader
        title="Overview"
        subtitle={
          <>
            {totals.accounts_reporting} of {totals.accounts} accounts reporting ·{" "}
            {plural(totals.open_positions, "open position")} · updated{" "}
            {age((Date.now() - dataUpdatedAt) / 1000)}
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
      {stored.length > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-warning)", color: "var(--ink)" }}
        >
          <strong>{stored.join(", ")}</strong>{" "}
          {stored.length === 1 ? "is" : "are"} showing their last recorded figures — their
          agent is not answering. Included in the totals, but not current.
        </div>
      )}

      {missing.length > 0 && (
        <div
          className="mb-4 rounded border px-3 py-2 text-sm"
          style={{ borderColor: "var(--status-warning)", color: "var(--ink)" }}
        >
          <strong>{missing.join(", ")}</strong> {missing.length === 1 ? "is" : "are"} not
          reporting — the totals below exclude {missing.length === 1 ? "it" : "them"}.
        </div>
      )}

      <div className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Stat
          label="Available cash"
          value={<Money>{money(totals.available)}</Money>}
          note={<><Money>{money(totals.utilised)}</Money> utilised</>}
        />
        <Stat
          label="Realised today"
          value={<Money>{signed(totals.realised_today)}</Money>}
          tone={pnlClass(totals.realised_today)}
          note="broker figure, marked from the previous close"
        />
        <Stat
          label="Unrealised — positions"
          value={<Money>{signed(totals.positions_unrealised)}</Money>}
          tone={pnlClass(totals.positions_unrealised)}
          note={`${plural(totals.open_positions, "position")} open · ${plural(
            totals.open_orders,
            "order",
          )} working`}
        />
        <Stat
          label="Unrealised — holdings"
          value={<Money>{signed(totals.holdings_unrealised)}</Money>}
          tone={pnlClass(totals.holdings_unrealised)}
          note={<><Money>{money(totals.holdings_value)}</Money> at market</>}
        />
      </div>

      <Card className="overflow-x-auto">
        <table className="w-full min-w-[1000px]">
          <thead>
            <tr>
              <Header>Account</Header>
              <Header>Status</Header>
              <Header>Available</Header>
              <Header>Utilised</Header>
              <Header>Realised today</Header>
              <Header>Unrealised</Header>
              <Header>Positions</Header>
              <Header>Holdings value</Header>
              <Header>Holdings P&amp;L</Header>
              <Header>Orders</Header>
            </tr>
          </thead>
          <tbody>
            {accounts.map((row) => (
              <AccountRowView key={row.account} row={row} />
            ))}
          </tbody>
        </table>
      </Card>

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        Realised today is the broker's own day figure, marked from the previous close. A
        position's own realised P&amp;L covers the whole life of the trade and is a
        different number — they are never added together.
      </p>
    </>
  );
}
