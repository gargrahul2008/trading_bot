export interface SectionMeta {
  age_s: number | null;
  stale: boolean | null;
  error: string | null;
}

export interface Funds {
  available: number;
  utilised: number;
  total: number;
  /** Broker's own figure for today, marked from the previous close. Not the
   *  same as the sum of positions' realised, which covers each trade's whole
   *  life — the two never share a column. */
  realised_today: number;
}

export interface PositionsSummary {
  open: number;
  long: number;
  short: number;
  /** Stock sold out of holdings and awaiting settlement — a negative position,
   *  but not a short anyone has to buy back. */
  delivery_sales: number;
  carried: number;
  opened_today: number;
  derivatives: number;
  unrealised: number;
  realised: number;
}

export interface HoldingsSummary {
  count: number;
  invested: number;
  market_value: number;
  unrealised: number;
  sold_today: number;
}

export interface OrdersSummary {
  total: number;
  open: number;
  rejected: number;
  by_source: Record<string, number>;
}

export interface AccountRow {
  account: string;
  reachable: boolean;
  live: boolean;
  /** False when the broker is rejecting the access token. Distinct from
   *  unreachable: the agent is fine, its credentials are not. */
  auth_ok?: boolean;
  /** True when this row was rebuilt from the store because the agent could not
   *  be reached. The figures are real but not current. */
  from_store: boolean;
  agent_error?: string;
  phase?: string | null;
  allow_trading?: boolean;
  error: string | null;
  funds: Funds | null;
  positions: PositionsSummary | null;
  holdings: HoldingsSummary | null;
  orders: OrdersSummary | null;
  sections: Record<string, SectionMeta>;
}

export interface Totals {
  accounts: number;
  accounts_reporting: number;
  accounts_missing: string[];
  accounts_from_store: string[];
  available: number;
  utilised: number;
  realised_today: number;
  positions_unrealised: number;
  holdings_unrealised: number;
  holdings_value: number;
  open_positions: number;
  open_orders: number;
}

export interface Overview {
  accounts: AccountRow[];
  totals: Totals;
  configured: boolean;
}

export interface RealisedDetail {
  gross: string;
  charges: string;
  net: string;
  available: boolean;
}

/** Money arrives as decimal strings, not JSON floats — these are computed
 *  exactly and must not be rounded by the browser on the way in. */
export interface PortfolioRow {
  account: string;
  capital_in: string;
  free: string;
  utilised: string;
  /** Cost of what is open — what was paid, not what it is worth now. */
  deployed: string;
  market_value: string;
  /** Notional sold short. Margin, not deployed capital, so reported apart. */
  short_exposure: string;
  unrealised: string;
  realised: string;
  realised_is_partial: boolean;
  realised_detail: RealisedDetail;
  net_worth: string;
  pnl: string;
  /** null, not 0, when no capital has been imported — 0% reads as a fact and
   *  this is the absence of one. */
  return_pct: string | null;
  counts: { positions: number; long: number; short: number; holdings: number };
  from_store: boolean;
  reachable: boolean;
  error: string | null;
}

export interface PortfolioTotals {
  accounts: number;
  capital_in: string;
  free: string;
  utilised: string;
  deployed: string;
  market_value: string;
  short_exposure: string;
  unrealised: string;
  realised: string;
  realised_is_partial: boolean;
  net_worth: string;
  pnl: string;
  return_pct: string | null;
  counts: { positions: number; long: number; short: number; holdings: number };
}

export interface Portfolio {
  accounts: PortfolioRow[];
  totals: PortfolioTotals | null;
  fy_start: string;
  configured: boolean;
}
