import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Card } from "./ui";
import { Th } from "./DataTable";
import { api } from "../lib/api";
import { num } from "../lib/format";
import type { LimitsPayload } from "../lib/types";

/** Rules in the order they bite. Every one bounds a mistake — a quantity typed
 *  with an extra zero, a retry that does not know it succeeded — rather than a
 *  strategy, which is why the defaults are generous. */
const ORDER = [
  "max_order_value",
  "max_symbol_exposure",
  "max_daily_loss",
  "max_orders_per_minute",
] as const;

const LABEL: Record<string, string> = {
  max_order_value: "Per order",
  max_symbol_exposure: "Per scrip",
  max_daily_loss: "Daily loss",
  max_orders_per_minute: "Orders/min",
};

function Cell({ account, rule, value }: { account: string; rule: string; value: string }) {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState<string | null>(null);
  const save = useMutation({
    mutationFn: (next: string) =>
      api.post("/limits", { account, name: rule, value: next }),
    onSuccess: () => {
      setDraft(null);
      void queryClient.invalidateQueries({ queryKey: ["limits"] });
    },
  });

  const off = (num(value) ?? 0) <= 0;
  return (
    <td className="px-3 py-2 text-right">
      <input
        value={draft ?? (off ? "" : value)}
        placeholder="off"
        onChange={(event) => setDraft(event.target.value)}
        onBlur={() => draft !== null && draft !== value && save.mutate(draft || "0")}
        onKeyDown={(event) => event.key === "Enter" && event.currentTarget.blur()}
        inputMode="numeric"
        className="tnum w-28 rounded border bg-transparent px-2 py-1 text-right text-sm"
        style={{ borderColor: save.isError ? "var(--status-critical)" : "var(--border)" }}
        title={save.isError ? String(save.error) : "0 or blank turns this rule off"}
      />
    </td>
  );
}

/** Risk limits, editable.
 *
 *  They are enforced in the API, which is the only path to a broker — this
 *  table is where they are set, not where they are applied. A rule the browser
 *  enforced could be skipped by opening the network tab.
 */
export function RiskLimits({ accounts }: { accounts: string[] }) {
  const [open, setOpen] = useState(false);
  const { data } = useQuery({
    queryKey: ["limits"],
    queryFn: () => api.get<LimitsPayload>("/limits"),
    staleTime: 60_000,
  });

  if (!data?.available) return null;

  return (
    <Card className="mt-4 overflow-x-auto">
      <button
        onClick={() => setOpen((on) => !on)}
        className="flex w-full items-baseline gap-2 px-4 py-2.5 text-left"
      >
        <span className="text-sm font-semibold">{open ? "▾" : "▸"} Risk limits</span>
        <span className="text-xs text-[var(--ink-muted)]">
          checked before every order leaves this machine
        </span>
      </button>

      {open && (
        <>
          <table className="w-full min-w-[720px]">
            <thead>
              <tr>
                <Th align="left">Account</Th>
                {ORDER.map((rule) => (
                  <Th key={rule} help={data.rules[rule]}>
                    {LABEL[rule]}
                  </Th>
                ))}
              </tr>
            </thead>
            <tbody>
              {accounts.map((account) => (
                <tr key={account} className="border-t" style={{ borderColor: "var(--hairline)" }}>
                  <td className="px-3 py-2 text-sm font-medium">{account}</td>
                  {ORDER.map((rule) => (
                    <Cell
                      key={rule}
                      account={account}
                      rule={rule}
                      value={data.limits[account]?.[rule] ?? "0"}
                    />
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          <p className="px-4 py-2 text-xs text-[var(--ink-muted)]">
            Blank or 0 turns a rule off — which is not the same as unset, and does not
            look the same. A refused order never reaches the broker and is still recorded,
            because what the limits stop is the reason to keep or change them. Closing
            orders are exempt from the daily loss limit: a rule that stops someone cutting
            a losing position is the trap it was meant to prevent.
          </p>
        </>
      )}
    </Card>
  );
}
