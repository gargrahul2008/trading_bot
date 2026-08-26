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
