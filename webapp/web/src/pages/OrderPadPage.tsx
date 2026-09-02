import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { RecentTrades } from "../components/RecentTrades";
import { SymbolInput } from "../components/SymbolInput";
import type { SymbolMatch } from "../components/SymbolInput";
import { api, ApiError } from "../lib/api";
import { money, num } from "../lib/format";
import { Money } from "../lib/privacy";
import type { AuditEntry, LimitsPayload, Overview, PlaceRequest } from "../lib/types";

/** BO is ours, not the broker's.
 *
 *  Fyers deprecated BO and CO as product types — an order sent with one is
 *  refused outright ("-55 ... please use stopLoss and takeProfit fields"). The
 *  fields themselves were never the problem and were always being sent, so the
 *  bracket survives as what it always meant: an intraday order that carries its
 *  own stop and target. Picking it sends INTRADAY and requires both legs.
 *
 *  Kept as a name rather than dropped because it states an intention that
 *  INTRADAY does not: entry, stop and target decided together, before the
 *  position exists. The review line names the product actually sent, so the
 *  convenience never becomes a lie about what went to the broker.
 */
const PRODUCTS = ["BO", "INTRADAY", "CNC", "MARGIN", "MTF"] as const;

/** Ours -> the broker's. Anything absent is sent as itself. */
const SENT_AS: Record<string, string> = { BO: "INTRADAY" };

/** Products on which Fyers acts on an attached stop and target. On a delivery
 *  order it accepts them and does nothing, which is worse than refusing. */
const BRACKET_PRODUCTS = ["BO", "INTRADAY", "MARGIN"];
const TYPES = [
  { value: "MARKET", label: "Market" },
  { value: "LIMIT", label: "Limit" },
  { value: "SL", label: "SL" },
  { value: "SL_M", label: "SL-M" },
] as const;

/** Stop and target start here, as a percentage of the price, and are converted
 *  to the points Fyers actually takes. Typed over freely — a default is a
 *  starting point, not a policy. */
const DEFAULT_PCT = 2;

function needs(orderType: string, product: string) {
  const bracket = BRACKET_PRODUCTS.includes(product);
  return {
    limit: orderType === "LIMIT" || orderType === "SL",
    stop: orderType === "SL" || orderType === "SL_M",
    legs: bracket,
    target: bracket,
    // A bracket without both legs is just an intraday order under a name that
    // promises more. On INTRADAY and MARGIN they stay optional.
    legsRequired: product === "BO",
  };
}

const inputClass = "mt-1 w-full rounded border bg-transparent px-3 py-2 text-sm tnum";

/** Round to the instrument's tick.
 *
 *  Tick size is per instrument, not per exchange: 20MICRONS trades in paise and
 *  RELIANCE in ten-paise steps. A price off the tick is rejected by the
 *  exchange, so this is the difference between an order working and an order
 *  bouncing.
 */
function toTick(price: number, tick: number): number {
  if (!(tick > 0)) return price;
  const steps = Math.round(price / tick);
  // Back through a fixed number of decimals, or 0.1 * 3 reappears as
  // 0.30000000000000004 and the exchange sees an off-tick price.
  const decimals = (String(tick).split(".")[1] ?? "").length;
  return Number((steps * tick).toFixed(decimals));
}

/** What a points figure is as a percentage of the price it applies to. */
function asPercent(points: number, reference: number): string | null {
  if (!(points > 0) || !(reference > 0)) return null;
  return ((points / reference) * 100).toFixed(2) + "%";
}
const labelClass =
  "text-xs font-medium uppercase tracking-wide text-[var(--ink-muted)]";

export function OrderPadPage() {
  const queryClient = useQueryClient();
  const [account, setAccount] = useState("");
  const [symbol, setSymbol] = useState("");
  const [side, setSide] = useState<"BUY" | "SELL">("BUY");
  const [qty, setQty] = useState("");
  const [product, setProduct] = useState<string>("BO");
  const [orderType, setOrderType] = useState("MARKET");
  const [limitPrice, setLimitPrice] = useState("");
  const [stopPrice, setStopPrice] = useState("");
  const [stopLoss, setStopLoss] = useState("");
  const [takeProfit, setTakeProfit] = useState("");
  const [legsTouched, setLegsTouched] = useState(false);
  // The instrument itself, once picked: its tick is what prices must land on.
  const [instrument, setInstrument] = useState<SymbolMatch | null>(null);
  const [done, setDone] = useState<string | null>(null);
  // Sizing is collapsed by default: someone who already knows the quantity must
  // not have to dismiss a calculator to type it.
  const [sizing, setSizing] = useState(false);
  const [riskAmount, setRiskAmount] = useState("");
  const [deployAmount, setDeployAmount] = useState("");

  const overview = useQuery({
    queryKey: ["overview"],
    queryFn: () => api.get<Overview>("/overview"),
    refetchInterval: 15000,
  });
  // A quote costs a broker call, so it is fetched when the symbol settles
  // rather than on every keystroke.
  const settled = symbol.includes(":") && symbol.length > 6;
  const quote = useQuery({
    queryKey: ["quote", account, symbol],
    queryFn: () =>
      api.get<{ quotes: Record<string, number> }>(
        `/quote?account=${encodeURIComponent(account)}&symbols=${encodeURIComponent(symbol)}`,
      ),
    enabled: Boolean(account && settled),
    staleTime: 10_000,
    retry: false,
  });

  const ltp = quote.data?.quotes?.[symbol] ?? null;
  // A symbol the broker will not price is one it does not know. With no confirm
  // step, that guess — "relia" completing to NSE:RELIA-EQ — would otherwise go
  // straight out.
  const unpriced = Boolean(account && settled && !quote.isFetching && ltp === null);
  const number = (text: string) => (text.trim() === "" ? 0 : Number(text));

  // A market order has no price of its own, so the mark is what values the
  // trade and what the percentage legs are measured from.
  const reference =
    (needs(orderType, product).limit ? number(limitPrice) : 0) ||
    number(stopPrice) ||
    ltp ||
    0;

  // Fill the legs from the price once it is known, and leave them alone the
  // moment they are typed over.
  const tick = instrument?.tick_size ?? 0;
  useEffect(() => {
    if (!legsTouched && reference > 0) {
      // The legs are distances, and the exchange wants them on the tick too.
      const points = String(toTick((reference * DEFAULT_PCT) / 100, tick || 0.05));
      setStopLoss(points);
      setTakeProfit(points);
    }
  }, [reference, legsTouched, tick]);

  const place = useMutation({
    mutationFn: (request: PlaceRequest) => api.post("/orders", request),
    onSuccess: () => {
      setDone(`${side} ${qty} ${symbol} sent to ${account}.`);
      void queryClient.invalidateQueries();
    },
    onError: () => void queryClient.invalidateQueries({ queryKey: ["audit"] }),
  });

  // A symbol can arrive without passing through the dropdown — picked from the
  // Recent list, or pasted whole. Its tick has to be looked up anyway, or
  // toTick is handed a tick of zero and the price goes to the exchange
  // unrounded, which it rejects.
  const lookup = useQuery({
    queryKey: ["symbol", symbol],
    queryFn: () => api.get<SymbolMatch>(`/symbols/${encodeURIComponent(symbol)}`),
    enabled: settled && instrument?.symbol !== symbol,
    staleTime: 5 * 60_000,
    retry: false,
  });
  useEffect(() => {
    if (lookup.data && lookup.data.symbol === symbol) setInstrument(lookup.data);
  }, [lookup.data, symbol]);

  const limits = useQuery({
    queryKey: ["limits"],
    queryFn: () => api.get<LimitsPayload>("/limits"),
    staleTime: 60_000,
  });

  const audit = useQuery({
    queryKey: ["audit"],
    queryFn: () => api.get<{ entries: AuditEntry[] }>("/audit?limit=8"),
    refetchInterval: 10000,
  });

  if (overview.isError) return <ErrorNote error={overview.error} />;
  if (overview.isLoading || !overview.data) return <Loading what="accounts" />;

  const accounts = overview.data.accounts;
  const chosen = accounts.find((a) => a.account === account);
  const want = needs(orderType, product);
  const quantity = number(qty);
  const value = quantity > 0 && reference > 0 ? quantity * reference : null;

  const problems: string[] = [];
  if (!account) problems.push("choose an account");
  if (!symbol.trim()) problems.push("symbol");
  if (!Number.isInteger(quantity) || quantity <= 0) problems.push("quantity");
  if (want.limit && number(limitPrice) <= 0) problems.push("limit price");
  if (want.stop && number(stopPrice) <= 0) problems.push("trigger price");
  if (want.legsRequired && number(stopLoss) <= 0) problems.push("stop-loss");
  if (want.legsRequired && number(takeProfit) <= 0) problems.push("target");
  if (want.legs && reference > 0 && number(stopLoss) >= reference)
    problems.push("stop-loss must be points, not a price");
  if (want.target && reference > 0 && number(takeProfit) >= reference)
    problems.push("target must be points, not a price");
  if (unpriced) problems.push("the broker returned no price for this symbol");

  const request: PlaceRequest = {
    account,
    symbol: symbol.trim().toUpperCase(),
    side,
    qty: quantity,
    product_type: SENT_AS[product] ?? product,
    order_type: orderType,
    limit_price: want.limit ? toTick(number(limitPrice), tick) : 0,
    stop_price: want.stop ? toTick(number(stopPrice), tick) : 0,
    stop_loss: want.legs ? toTick(number(stopLoss), tick) : 0,
    take_profit: want.target ? toTick(number(takeProfit), tick) : 0,
  };

  // Shown before the button is pressed, so the server's refusal is a backstop
  // rather than the first anyone hears of the limit.
  const cap = num(limits.data?.limits?.[account]?.max_order_value ?? null) ?? 0;
  const overCap = cap > 0 && value !== null && value > cap;
  if (overCap) problems.push(`over ${account}'s ${money(cap)} per-order limit`);

  const ready = problems.length === 0 && !place.isPending;
  const tradingOff = chosen && !chosen.allow_trading;
  const error =
    place.error instanceof ApiError ? place.error.message : place.error ? String(place.error) : null;
  // A timeout is not a rejection. The order may be live at the broker, and the
  // one thing that must not happen next is a reflexive retry — which is what a
  // red "failed" box invites.
  const unknown = place.error instanceof ApiError && place.error.status === 504;

  return (
    <>
      <PageHeader
        title="Place an order"
        subtitle="The button says exactly what will be sent, and sends it."
      />

      {/* Every account's spendable cash, so choosing one is not a guess. */}
      <div className="mb-4 flex flex-wrap gap-2">
        {accounts.map((a) => (
          <button
            key={a.account}
            onClick={() => {
              setAccount(a.account);
              setDone(null);
            }}
            className="rounded border px-3 py-2 text-left"
            style={{
              borderColor: account === a.account ? "var(--accent)" : "var(--border)",
              borderWidth: account === a.account ? 2 : 1,
            }}
          >
            <div className="text-sm font-medium">
              {a.account}
              {!a.allow_trading && (
                <span className="ml-1.5 text-xs text-[var(--status-warning)]">read-only</span>
              )}
            </div>
            <div className="tnum text-xs text-[var(--ink-secondary)]">
              <Money>{money(a.funds?.available ?? null)}</Money> free
            </div>
          </button>
        ))}
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_300px]">
        <Card className="p-5">
          <div className="grid gap-4 sm:grid-cols-4">
            <div className="sm:col-span-2">
              <span className={labelClass}>Symbol</span>
              <SymbolInput
                value={symbol}
                onChange={(next) => {
                  setSymbol(next);
                  setDone(null);
                  if (next !== instrument?.symbol) setInstrument(null);
                }}
                onPick={(match) => {
                  setInstrument(match);
                  setDone(null);
                }}
                className={inputClass}
                autoFocus
              />
            </div>

            <div>
              <span className={labelClass}>
                Quantity{" "}
                <button
                  type="button"
                  onClick={() => setSizing((on) => !on)}
                  className="normal-case text-[var(--ink-muted)] hover:underline"
                >
                  {sizing ? "hide sizing" : "size it"}
                </button>
              </span>
              <input
                value={qty}
                onChange={(event) => {
                  setQty(event.target.value);
                  setDone(null);
                }}
                inputMode="numeric"
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              />
            </div>

            <div>
              <span className={labelClass}>
                Value {ltp !== null && orderType === "MARKET" && (
                  <span className="normal-case">at {money(ltp)}</span>
                )}
              </span>
              <div className="tnum mt-1 px-3 py-2 text-sm font-semibold">
                {value === null ? (
                  <span className="text-[var(--ink-muted)]">—</span>
                ) : (
                  <Money>{money(value)}</Money>
                )}
              </div>
            </div>

            {sizing && (
              <div className="col-span-full grid grid-cols-2 gap-3 rounded border px-3 py-3 md:grid-cols-4"
                   style={{ borderColor: "var(--hairline)" }}>
                <div>
                  <span className={labelClass}>Risk ₹</span>
                  <input
                    value={riskAmount}
                    onChange={(event) => {
                      setRiskAmount(event.target.value);
                      setDeployAmount("");
                      // Quantity is what the stop can afford: the amount you
                      // are willing to lose, divided by the distance to it.
                      const points = number(stopLoss);
                      const risk = number(event.target.value);
                      if (points > 0 && risk > 0) {
                        setQty(String(Math.max(Math.floor(risk / points), 0)));
                        setDone(null);
                      }
                    }}
                    inputMode="numeric"
                    placeholder="what you can lose"
                    className={inputClass}
                    style={{ borderColor: "var(--border)" }}
                  />
                </div>
                <div>
                  <span className={labelClass}>Deploy ₹</span>
                  <input
                    value={deployAmount}
                    onChange={(event) => {
                      setDeployAmount(event.target.value);
                      setRiskAmount("");
                      const money_ = number(event.target.value);
                      if (reference > 0 && money_ > 0) {
                        setQty(String(Math.max(Math.floor(money_ / reference), 0)));
                        setDone(null);
                      }
                    }}
                    inputMode="numeric"
                    placeholder="what you can commit"
                    className={inputClass}
                    style={{ borderColor: "var(--border)" }}
                  />
                </div>
                <div className="col-span-2 self-end text-xs text-[var(--ink-muted)]">
                  {riskAmount && number(stopLoss) > 0 ? (
                    <>
                      {money(number(riskAmount))} risked over a{" "}
                      {number(stopLoss)}-point stop is {qty || 0} shares
                      {reference > 0 && (
                        <> — {money(number(qty) * reference)} committed</>
                      )}
                      .
                    </>
                  ) : deployAmount && reference > 0 ? (
                    <>
                      {money(number(deployAmount))} at {money(reference)} is {qty || 0}{" "}
                      shares
                      {number(stopLoss) > 0 && (
                        <> — {money(number(qty) * number(stopLoss))} at risk</>
                      )}
                      .
                    </>
                  ) : (
                    "Risk needs a stop-loss; deploy needs a price. Whichever you type sets the quantity."
                  )}
                </div>
              </div>
            )}

            <div>
              <span className={labelClass}>Product</span>
              <select
                value={product}
                onChange={(event) => setProduct(event.target.value)}
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              >
                {PRODUCTS.map((p) => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
            </div>

            <div>
              <span className={labelClass}>Type</span>
              <select
                value={orderType}
                onChange={(event) => setOrderType(event.target.value)}
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              >
                {TYPES.map((t) => (
                  <option key={t.value} value={t.value}>{t.label}</option>
                ))}
              </select>
            </div>

            {want.limit && (
              <div>
                <span className={labelClass}>Limit</span>
                <input
                  value={limitPrice}
                  onChange={(event) => setLimitPrice(event.target.value)}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </div>
            )}

            {want.stop && (
              <div>
                <span className={labelClass}>Trigger</span>
                <input
                  value={stopPrice}
                  onChange={(event) => setStopPrice(event.target.value)}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </div>
            )}

            {want.legs && (
              <div>
                <span className={labelClass}>
                  Stop-loss (pts){" "}
                  <span className="normal-case text-[var(--ink-secondary)]">
                    {asPercent(number(stopLoss), reference) ?? ""}
                  </span>
                </span>
                <input
                  value={stopLoss}
                  onChange={(event) => {
                    setStopLoss(event.target.value);
                    setLegsTouched(true);
                  }}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </div>
            )}

            {want.target && (
              <div>
                <span className={labelClass}>
                  Target (pts){" "}
                  <span className="normal-case text-[var(--ink-secondary)]">
                    {asPercent(number(takeProfit), reference) ?? ""}
                  </span>
                </span>
                <input
                  value={takeProfit}
                  onChange={(event) => {
                    setTakeProfit(event.target.value);
                    setLegsTouched(true);
                  }}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </div>
            )}
          </div>

          {/* The review, in place. There is no separate confirm step, so this
              and the button below are what stands between a typo and a trade —
              which is why the button repeats the account and the whole order. */}
          <div
            className="mt-4 rounded px-3 py-2 text-sm"
            style={{ background: "color-mix(in srgb, var(--ink) 4%, var(--surface))" }}
          >
            {problems.length > 0 ? (
              <span className="text-[var(--ink-muted)]">Needs: {problems.join(", ")}</span>
            ) : (
              <span>
                <strong>{side}</strong> {quantity} <strong>{request.symbol}</strong> ·{" "}
                {product}
                {SENT_AS[product] && (
                  <span
                    className="text-[var(--ink-muted)]"
                    title="BO is ours — Fyers no longer has it as a product type"
                  >
                    {" "}
                    (sent as {SENT_AS[product]})
                  </span>
                )}{" "}
                {TYPES.find((t) => t.value === orderType)?.label}
                {want.limit && ` @ ${money(request.limit_price!)}`}
                {want.stop && ` trigger ${money(request.stop_price!)}`}
                {want.legs && ` · SL ${request.stop_loss} pts`}
                {want.target && ` · target ${request.take_profit} pts`}
                {value !== null && (
                  <>
                    {" · "}
                    <Money>{money(value)}</Money>
                  </>
                )}
              </span>
            )}
          </div>

          {unpriced && (
            <div
              className="mt-3 rounded border px-3 py-2 text-sm"
              style={{ borderColor: "var(--status-critical)" }}
            >
              No price came back for <strong>{symbol}</strong>. Either the broker does not
              know that symbol — a completion like <code>NSE:RELIA-EQ</code> looks right and
              is not — or the quote could not be fetched. Check the symbol before placing.
            </div>
          )}

          {tradingOff && (
            <div
              className="mt-3 rounded border px-3 py-2 text-sm"
              style={{ borderColor: "var(--status-warning)" }}
            >
              <strong>{account}</strong>’s agent is read-only — this order will be refused.
            </div>
          )}

          {error && (
            <div
              className="mt-3 rounded border px-3 py-2 text-sm"
              style={{
                borderColor: unknown ? "var(--status-warning)" : "var(--status-critical)",
                color: unknown ? "var(--ink)" : "var(--status-critical)",
              }}
            >
              {unknown && <strong>Outcome unknown. </strong>}
              {error}
            </div>
          )}

          {done && !error && (
            <div
              className="mt-3 rounded border px-3 py-2 text-sm"
              style={{ borderColor: "var(--status-good)" }}
            >
              {done}
            </div>
          )}

          <div className="mt-4 grid grid-cols-2 gap-3">
            {(["BUY", "SELL"] as const).map((option) => (
              <button
                key={option}
                onClick={() => {
                  setSide(option);
                  setDone(null);
                  if (problems.length === 0) place.mutate({ ...request, side: option });
                }}
                disabled={!ready}
                className="rounded px-3 py-3 text-sm font-semibold text-white disabled:opacity-40"
                style={{ background: option === "BUY" ? "var(--gain)" : "var(--loss)" }}
              >
                {place.isPending && side === option
                  ? "Sending…"
                  : ready
                    ? `${option} ${quantity} ${request.symbol}${account ? ` · ${account}` : ""}`
                    : option}
              </button>
            ))}
          </div>
        </Card>

        <Card className="p-4">
          <h2 className="text-sm font-semibold">Recent actions</h2>
          <ul className="mt-3 space-y-2 text-xs">
            {(audit.data?.entries ?? []).map((entry) => (
              <li key={entry.id} className="border-b pb-2" style={{ borderColor: "var(--hairline)" }}>
                <div className="flex justify-between gap-2">
                  <span className="font-medium">{entry.account}</span>
                  <span
                    style={{
                      color:
                        entry.result === "ok"
                          ? "var(--status-good)"
                          : entry.result === "error"
                            ? "var(--status-critical)"
                            : entry.result === "unknown"
                              ? "var(--status-warning)"
                              : "var(--ink-muted)",
                    }}
                  >
                    {entry.result}
                  </span>
                </div>
                <div className="text-[var(--ink-secondary)]">{entry.summary}</div>
                {(entry.result === "error" || entry.result === "unknown") && entry.message && (
                  <div
                    style={{
                      color:
                        entry.result === "unknown"
                          ? "var(--status-warning)"
                          : "var(--status-critical)",
                    }}
                  >
                    {entry.message.slice(0, 110)}
                  </div>
                )}
              </li>
            ))}
            {!(audit.data?.entries ?? []).length && (
              <li className="text-[var(--ink-muted)]">Nothing yet.</li>
            )}
          </ul>
        </Card>
      </div>

      {/* Clicking a line loads its account and symbol into the pad — adding to a
          position or closing one is the commonest reason to open this page. */}
      <RecentTrades
        onPick={(pickedAccount, pickedSymbol) => {
          setAccount(pickedAccount);
          setSymbol(pickedSymbol);
          setDone(null);
        }}
      />

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        BUY and SELL place immediately — there is no second confirmation, so the line above
        and the button text are the check. Stop-loss and target default to {DEFAULT_PCT}% of
        the price and are sent as points from the entry; typing over either one stops them
        following the price. BO is ours, not the broker's: Fyers deprecated bracket and cover
        as product types, so it is sent as {SENT_AS.BO} carrying the stop and target on the
        order itself — which is what a bracket always was. It requires both legs; on plain
        {" " + BRACKET_PRODUCTS.filter((p) => !SENT_AS[p]).join(" and ")} they are optional.
        On a delivery order Fyers accepts them and acts on neither, so the boxes are hidden
        rather than offering protection that would not exist.
      </p>
    </>
  );
}
