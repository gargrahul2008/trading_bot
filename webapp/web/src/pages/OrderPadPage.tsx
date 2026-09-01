import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Card, ErrorNote, Loading, PageHeader } from "../components/ui";
import { Confirm } from "../components/Confirm";
import { api, ApiError } from "../lib/api";
import { money } from "../lib/format";
import type { AuditEntry, Overview, PlaceRequest } from "../lib/types";

const PRODUCTS = ["CNC", "INTRADAY", "MARGIN", "MTF", "BO", "CO"] as const;
const TYPES = [
  { value: "MARKET", label: "Market" },
  { value: "LIMIT", label: "Limit" },
  { value: "SL", label: "SL (stop-limit)" },
  { value: "SL_M", label: "SL-M (stop-market)" },
] as const;

/** Which price boxes a given order actually uses.
 *
 *  Fields that would be ignored are hidden rather than disabled: an empty box
 *  someone can type into is an invitation to believe it did something.
 */
function needs(orderType: string, product: string) {
  return {
    limit: orderType === "LIMIT" || orderType === "SL",
    stop: orderType === "SL" || orderType === "SL_M",
    // Bracket and cover carry their exit with them; on any other product the
    // broker drops these silently.
    legs: product === "BO" || product === "CO",
    target: product === "BO",
  };
}

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block">
      <span className="text-xs font-medium uppercase tracking-wide text-[var(--ink-muted)]">
        {label}
      </span>
      {children}
      {hint && <span className="mt-0.5 block text-xs text-[var(--ink-muted)]">{hint}</span>}
    </label>
  );
}

const inputClass =
  "mt-1 w-full rounded border bg-transparent px-3 py-2 text-sm tnum";

export function OrderPadPage() {
  const queryClient = useQueryClient();
  const [account, setAccount] = useState("");
  const [symbol, setSymbol] = useState("");
  const [side, setSide] = useState<"BUY" | "SELL">("BUY");
  const [qty, setQty] = useState("");
  const [product, setProduct] = useState("CNC");
  const [orderType, setOrderType] = useState("LIMIT");
  const [limitPrice, setLimitPrice] = useState("");
  const [stopPrice, setStopPrice] = useState("");
  const [stopLoss, setStopLoss] = useState("");
  const [takeProfit, setTakeProfit] = useState("");
  const [confirming, setConfirming] = useState(false);
  const [done, setDone] = useState<string | null>(null);

  const overview = useQuery({
    queryKey: ["overview"],
    queryFn: () => api.get<Overview>("/overview"),
  });
  const audit = useQuery({
    queryKey: ["audit"],
    queryFn: () => api.get<{ entries: AuditEntry[] }>("/audit?limit=10"),
    refetchInterval: 10000,
  });

  const place = useMutation({
    mutationFn: (request: PlaceRequest) => api.post("/orders", request),
    onSuccess: () => {
      setConfirming(false);
      setDone("Order sent to " + account + ".");
      void queryClient.invalidateQueries();
    },
    onError: () => {
      setConfirming(false);
      // A refusal is exactly what the log is for, so refresh it here too — the
      // success path is not the only one worth seeing.
      void queryClient.invalidateQueries({ queryKey: ["audit"] });
    },
  });

  if (overview.isError) return <ErrorNote error={overview.error} />;
  if (overview.isLoading || !overview.data) return <Loading what="accounts" />;

  const accounts = overview.data.accounts;
  const chosen = accounts.find((a) => a.account === account);
  const want = needs(orderType, product);
  const number = (text: string) => (text.trim() === "" ? 0 : Number(text));

  const problems: string[] = [];
  if (!account) problems.push("choose an account");
  if (!symbol.trim()) problems.push("enter a symbol");
  if (!qty.trim() || !Number.isInteger(number(qty)) || number(qty) <= 0)
    problems.push("quantity must be a whole number above zero");
  if (want.limit && number(limitPrice) <= 0) problems.push("limit price is required");
  if (want.stop && number(stopPrice) <= 0) problems.push("trigger price is required");
  if (want.legs && number(stopLoss) <= 0) problems.push("stop-loss is required");
  if (want.target && number(takeProfit) <= 0) problems.push("target is required");

  const reference = number(limitPrice) || number(stopPrice);
  if (want.legs && reference > 0 && number(stopLoss) >= reference)
    problems.push("stop-loss is in points from the entry, not a price");
  if (want.target && reference > 0 && number(takeProfit) >= reference)
    problems.push("target is in points from the entry, not a price");

  const request: PlaceRequest = {
    account,
    symbol: symbol.trim().toUpperCase(),
    side,
    qty: number(qty),
    product_type: product,
    order_type: orderType,
    limit_price: want.limit ? number(limitPrice) : 0,
    stop_price: want.stop ? number(stopPrice) : 0,
    stop_loss: want.legs ? number(stopLoss) : 0,
    take_profit: want.target ? number(takeProfit) : 0,
  };

  const tradingOff = chosen && !chosen.allow_trading;
  const error =
    place.error instanceof ApiError ? place.error.message : place.error ? String(place.error) : null;

  return (
    <>
      <PageHeader
        title="Place an order"
        subtitle="Goes to the account you choose. Nothing is sent until you confirm."
      />

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
        <Card className="p-5">
          <div className="grid gap-4 sm:grid-cols-2">
            <Field label="Account" hint="Never remembered between orders — choose it each time.">
              <select
                value={account}
                onChange={(event) => {
                  setAccount(event.target.value);
                  setDone(null);
                }}
                className={inputClass}
                style={{ borderColor: account ? "var(--border)" : "var(--status-warning)" }}
              >
                <option value="">— choose —</option>
                {accounts.map((a) => (
                  <option key={a.account} value={a.account}>
                    {a.account}
                  </option>
                ))}
              </select>
            </Field>

            <Field label="Symbol" hint="As the broker writes it, e.g. NSE:RELIANCE-EQ">
              <input
                value={symbol}
                onChange={(event) => setSymbol(event.target.value)}
                placeholder="NSE:RELIANCE-EQ"
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              />
            </Field>

            <Field label="Side">
              <div className="mt-1 flex gap-2">
                {(["BUY", "SELL"] as const).map((option) => (
                  <button
                    key={option}
                    onClick={() => setSide(option)}
                    className="flex-1 rounded border px-3 py-2 text-sm font-semibold"
                    style={{
                      borderColor: side === option ? "transparent" : "var(--border)",
                      background:
                        side === option
                          ? option === "BUY"
                            ? "var(--gain)"
                            : "var(--loss)"
                          : "transparent",
                      color: side === option ? "#fff" : "var(--ink-secondary)",
                    }}
                  >
                    {option}
                  </button>
                ))}
              </div>
            </Field>

            <Field label="Quantity">
              <input
                value={qty}
                onChange={(event) => setQty(event.target.value)}
                inputMode="numeric"
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              />
            </Field>

            <Field label="Product">
              <select
                value={product}
                onChange={(event) => setProduct(event.target.value)}
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              >
                {PRODUCTS.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </Field>

            <Field label="Order type">
              <select
                value={orderType}
                onChange={(event) => setOrderType(event.target.value)}
                className={inputClass}
                style={{ borderColor: "var(--border)" }}
              >
                {TYPES.map((t) => (
                  <option key={t.value} value={t.value}>
                    {t.label}
                  </option>
                ))}
              </select>
            </Field>

            {want.limit && (
              <Field label="Limit price">
                <input
                  value={limitPrice}
                  onChange={(event) => setLimitPrice(event.target.value)}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </Field>
            )}

            {want.stop && (
              <Field label="Trigger price" hint="The order activates here.">
                <input
                  value={stopPrice}
                  onChange={(event) => setStopPrice(event.target.value)}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </Field>
            )}

            {want.legs && (
              <Field label="Stop-loss (points)" hint="Distance from the entry, not a price.">
                <input
                  value={stopLoss}
                  onChange={(event) => setStopLoss(event.target.value)}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </Field>
            )}

            {want.target && (
              <Field label="Target (points)" hint="Distance from the entry, not a price.">
                <input
                  value={takeProfit}
                  onChange={(event) => setTakeProfit(event.target.value)}
                  inputMode="decimal"
                  className={inputClass}
                  style={{ borderColor: "var(--border)" }}
                />
              </Field>
            )}
          </div>

          {tradingOff && (
            <div
              className="mt-4 rounded border px-3 py-2 text-sm"
              style={{ borderColor: "var(--status-warning)" }}
            >
              <strong>{account}</strong>’s agent is running read-only, so this order would
              be refused. Enable it deliberately with{" "}
              <code>ALLOW_TRADING=1 python3 deploy/gen_systemd_units.py</code>.
            </div>
          )}

          {/* Success first: clearing the form used to invalidate it, so the
              confirmation appeared underneath a complaint about the very order
              that had just gone through. */}
          {done && !error && (
            <div
              className="mt-4 rounded border px-3 py-2 text-sm"
              style={{ borderColor: "var(--status-good)" }}
            >
              {done} Edit and review again to place another.
            </div>
          )}

          {problems.length > 0 && !done && (
            <p className="mt-4 text-sm text-[var(--ink-muted)]">
              Before this can be sent: {problems.join(" · ")}.
            </p>
          )}

          {error && (
            <div
              className="mt-4 rounded border px-3 py-2 text-sm"
              style={{ borderColor: "var(--status-critical)", color: "var(--status-critical)" }}
            >
              {error}
            </div>
          )}

          <button
            onClick={() => {
              setDone(null);
              setConfirming(true);
            }}
            disabled={problems.length > 0}
            className="mt-4 w-full rounded px-3 py-2.5 text-sm font-semibold text-white disabled:opacity-40"
            style={{ background: side === "BUY" ? "var(--gain)" : "var(--loss)" }}
          >
            Review {side.toLowerCase()} order
          </button>
        </Card>

        <Card className="p-5">
          <h2 className="text-sm font-semibold">Recent actions</h2>
          <p className="mt-1 text-xs text-[var(--ink-muted)]">
            Everything placed, changed or cancelled from this dashboard — recorded before
            it reached the broker.
          </p>
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
                  <div className="text-[var(--status-critical)]">{entry.message.slice(0, 120)}</div>
                )}
              </li>
            ))}
            {!(audit.data?.entries ?? []).length && (
              <li className="text-[var(--ink-muted)]">Nothing yet.</li>
            )}
          </ul>
        </Card>
      </div>

      {confirming && (
        <Confirm
          title={`${side} ${request.qty} ${request.symbol}`}
          account={account}
          danger={side === "SELL"}
          busy={place.isPending}
          confirmLabel={`Place ${side.toLowerCase()} order`}
          lines={[
            { label: "Symbol", value: request.symbol },
            { label: "Side", value: side },
            { label: "Quantity", value: request.qty },
            { label: "Product", value: product },
            { label: "Type", value: TYPES.find((t) => t.value === orderType)?.label },
            ...(want.limit ? [{ label: "Limit", value: money(request.limit_price!) }] : []),
            ...(want.stop ? [{ label: "Trigger", value: money(request.stop_price!) }] : []),
            ...(want.legs
              ? [{ label: "Stop-loss", value: `${request.stop_loss} pts from entry` }]
              : []),
            ...(want.target
              ? [{ label: "Target", value: `${request.take_profit} pts from entry` }]
              : []),
          ]}
          warning={
            orderType === "MARKET"
              ? "A market order fills at whatever price is available, which may be well away from the last traded price."
              : undefined
          }
          onCancel={() => setConfirming(false)}
          onConfirm={() => place.mutate(request)}
        />
      )}
    </>
  );
}
