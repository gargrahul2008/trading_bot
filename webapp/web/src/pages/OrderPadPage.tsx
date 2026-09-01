import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { SymbolInput } from "../components/SymbolInput";
import { api, ApiError } from "../lib/api";
import { money } from "../lib/format";
import { Money } from "../lib/privacy";
import type { AuditEntry, Overview, PlaceRequest } from "../lib/types";

const PRODUCTS = ["BO", "CNC", "INTRADAY", "MARGIN", "MTF", "CO"] as const;
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
  return {
    limit: orderType === "LIMIT" || orderType === "SL",
    stop: orderType === "SL" || orderType === "SL_M",
    legs: product === "BO" || product === "CO",
    target: product === "BO",
  };
}

const inputClass = "mt-1 w-full rounded border bg-transparent px-3 py-2 text-sm tnum";
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
  const [done, setDone] = useState<string | null>(null);

  const overview = useQuery({
    queryKey: ["overview"],
    queryFn: () => api.get<Overview>("/overview"),
    refetchInterval: 15000,
  });
  const symbols = useQuery({
    queryKey: ["symbols"],
    queryFn: () => api.get<{ symbols: string[] }>("/symbols"),
    staleTime: 5 * 60_000,
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
  useEffect(() => {
    if (!legsTouched && reference > 0) {
      const points = ((reference * DEFAULT_PCT) / 100).toFixed(2);
      setStopLoss(points);
      setTakeProfit(points);
    }
  }, [reference, legsTouched]);

  const place = useMutation({
    mutationFn: (request: PlaceRequest) => api.post("/orders", request),
    onSuccess: () => {
      setDone(`${side} ${qty} ${symbol} sent to ${account}.`);
      void queryClient.invalidateQueries();
    },
    onError: () => void queryClient.invalidateQueries({ queryKey: ["audit"] }),
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
  if (want.legs && number(stopLoss) <= 0) problems.push("stop-loss");
  if (want.target && number(takeProfit) <= 0) problems.push("target");
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
    product_type: product,
    order_type: orderType,
    limit_price: want.limit ? number(limitPrice) : 0,
    stop_price: want.stop ? number(stopPrice) : 0,
    stop_loss: want.legs ? number(stopLoss) : 0,
    take_profit: want.target ? number(takeProfit) : 0,
  };

  const ready = problems.length === 0 && !place.isPending;
  const tradingOff = chosen && !chosen.allow_trading;
  const error =
    place.error instanceof ApiError ? place.error.message : place.error ? String(place.error) : null;

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
                }}
                known={symbols.data?.symbols ?? []}
                className={inputClass}
                autoFocus
              />
            </div>

            <div>
              <span className={labelClass}>Quantity</span>
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
                <span className={labelClass}>Stop-loss (pts)</span>
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
                <span className={labelClass}>Target (pts)</span>
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
                {product} {TYPES.find((t) => t.value === orderType)?.label}
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
              style={{ borderColor: "var(--status-critical)", color: "var(--status-critical)" }}
            >
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
                            : "var(--ink-muted)",
                    }}
                  >
                    {entry.result}
                  </span>
                </div>
                <div className="text-[var(--ink-secondary)]">{entry.summary}</div>
                {entry.result === "error" && entry.message && (
                  <div className="text-[var(--status-critical)]">
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

      <p className="mt-3 text-xs text-[var(--ink-muted)]">
        BUY and SELL place immediately — there is no second confirmation, so the line above
        and the button text are the check. Stop-loss and target default to {DEFAULT_PCT}% of
        the price and are sent as points from the entry; typing over either one stops them
        following the price.
      </p>
    </>
  );
}
