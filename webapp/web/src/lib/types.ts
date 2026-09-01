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
  /** Deployed exceeds capital in. Either leverage, or a base missing the
   *  securities the account already held at the start of the year. */
  deployed_exceeds_capital: boolean;
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

export interface Position {
  account: string;
  symbol: string;
  net_qty: number;
  direction: string;
  avg_price: number;
  ltp: number;
  unrealised: number;
  realised: number;
  product_type: string;
  kind: string;
  /** Which broker book it came from. Fyers keeps settled delivery stock in
   *  holdings and everything else in positions; both are money at risk. */
  book: "position" | "holding";
  /** Stock sold out of holdings, awaiting settlement — not a short. */
  delivery_sale?: boolean;
  is_derivative?: boolean;
  carried?: boolean;
  opened_today?: boolean;
  from_store: boolean;
  stale: boolean;
  age_s: number | null;
}

export interface PositionsPayload {
  positions: Position[];
  /** Every account queried — not only those holding something. An account with
   *  nothing open still gets a column, or "empty" and "unread" look alike. */
  accounts: string[];
  accounts_missing: string[];
  /** Sold out of holdings today, awaiting settlement. Not open risk — their
   *  P&L is realised and appears on Trades. */
  sold_today: { account: string; symbol: string; qty: number }[];
}

export interface Trade {
  account: string;
  symbol: string;
  direction: string;
  kind: string;
  qty: string;
  entry_price: string;
  exit_price: string;
  opened_day: string;
  closed_day: string;
  product_type: string;
  gross: string;
  /** null when the day's charges are unknown — never a confident zero. */
  charges: string | null;
  net: string | null;
  charges_estimated: boolean;
}

export interface TradeTotals {
  trades: number;
  gross: string;
  charges: string;
  net: string;
  trades_costed: number;
  trades_without_charges: number;
  charges_estimated: boolean;
}

export interface TradesPayload {
  trades: Trade[];
  accounts: string[];
  totals: TradeTotals;
  shown?: number;
  available: boolean;
}

export interface RealisedScrip {
  account: string;
  symbol: string;
  gross: string;
  /** null when a day's charges are unknown — never a confident zero. */
  charges: string | null;
  net: string | null;
  days: number;
  charges_estimated: boolean;
}

export interface RealisedPayload {
  scrips: RealisedScrip[];
  totals: {
    scrips: number;
    gross: string;
    charges: string;
    net: string;
    scrips_without_charges: number;
    days_without_charges: number;
  };
  accounts: string[];
  fy_start: string;
  available: boolean;
}

export interface Order {
  account: string;
  order_id: string;
  symbol: string;
  side: string;
  qty: number;
  filled_qty: number;
  remaining_qty?: number;
  limit_price: number;
  traded_price?: number;
  product_type: string;
  status: string;
  is_open: boolean;
  /** bot | manual | pending — who placed it. */
  source: string | null;
  run: string | null;
  matched_by: string | null;
  /** api | web, as the broker stamps it. */
  channel?: string | null;
  order_tag?: string | null;
  placed_at?: string | null;
  trading_day: string;
  message?: string | null;
  /** The reject's cause, from the parser the bots use. */
  kind?: string | null;
  reason?: string | null;
  live: boolean;
}

export interface OrdersPayload {
  orders: Order[];
  accounts: string[];
  available: boolean;
}

export interface PlaceRequest {
  account: string;
  symbol: string;
  side: "BUY" | "SELL";
  qty: number;
  product_type: string;
  order_type: string;
  limit_price?: number;
  stop_price?: number;
  stop_loss?: number;
  take_profit?: number;
  validity?: string;
}

export interface AuditEntry {
  id: number;
  at: number;
  action: string;
  account: string;
  summary: string;
  result: string;
  message: string | null;
}
