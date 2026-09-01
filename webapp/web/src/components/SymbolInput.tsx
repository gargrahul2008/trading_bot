import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../lib/api";

export interface SymbolMatch {
  symbol: string;
  name: string;
  short_name: string;
  tick_size: number;
  lot_size: number;
  exchange: string;
}

/** Symbol entry backed by the exchanges' own instrument list.
 *
 *  Searched on the server — 22,000 instruments is not a list to ship to a
 *  browser — and only once something has been typed. A dropdown that opens on
 *  focus with arbitrary suggestions is noise: nobody wants the first eight
 *  symbols alphabetically.
 */
export function SymbolInput({
  value,
  onChange,
  onPick,
  className,
  autoFocus,
}: {
  value: string;
  onChange: (symbol: string) => void;
  onPick?: (match: SymbolMatch) => void;
  className?: string;
  autoFocus?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [typed, setTyped] = useState("");
  const blurTimer = useRef<number>();

  // Debounced, so a query goes out per pause rather than per keystroke.
  useEffect(() => {
    const timer = window.setTimeout(() => setTyped(value.trim()), 140);
    return () => window.clearTimeout(timer);
  }, [value]);

  const { data } = useQuery({
    queryKey: ["symbol-search", typed],
    queryFn: () =>
      api.get<{ matches: SymbolMatch[] }>(`/symbols?q=${encodeURIComponent(typed)}`),
    // Nothing typed, nothing suggested.
    enabled: typed.length >= 1,
    staleTime: 60_000,
  });

  const matches = typed.length >= 1 ? (data?.matches ?? []) : [];

  function choose(match: SymbolMatch) {
    onChange(match.symbol);
    onPick?.(match);
    setOpen(false);
  }

  return (
    <div className="relative">
      <input
        value={value}
        autoFocus={autoFocus}
        onChange={(event) => {
          onChange(event.target.value.toUpperCase());
          setOpen(true);
          setActive(0);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          blurTimer.current = window.setTimeout(() => setOpen(false), 150);
        }}
        onKeyDown={(event) => {
          if (!open || !matches.length) return;
          if (event.key === "ArrowDown") {
            event.preventDefault();
            setActive((i) => Math.min(i + 1, matches.length - 1));
          } else if (event.key === "ArrowUp") {
            event.preventDefault();
            setActive((i) => Math.max(i - 1, 0));
          } else if (event.key === "Enter" || event.key === "Tab") {
            if (matches[active]) {
              event.preventDefault();
              choose(matches[active]);
            }
          } else if (event.key === "Escape") {
            setOpen(false);
          }
        }}
        placeholder="reliance"
        className={className}
        style={{ borderColor: "var(--border)" }}
      />

      {open && matches.length > 0 && (
        <ul
          className="absolute z-30 mt-1 max-h-72 w-full overflow-auto rounded border shadow-lg"
          style={{ background: "var(--surface)", borderColor: "var(--border)" }}
        >
          {matches.map((match, index) => (
            <li key={match.symbol}>
              <button
                onMouseDown={() => {
                  window.clearTimeout(blurTimer.current);
                  choose(match);
                }}
                onMouseEnter={() => setActive(index)}
                className={`block w-full px-3 py-1.5 text-left ${
                  index === active ? "bg-black/5 dark:bg-white/10" : ""
                }`}
              >
                <div className="flex items-baseline justify-between gap-3">
                  <span className="text-sm font-medium">{match.symbol}</span>
                  <span className="text-[10px] text-[var(--ink-muted)]">
                    tick {match.tick_size}
                    {match.lot_size > 1 && ` · lot ${match.lot_size}`}
                  </span>
                </div>
                <div className="truncate text-xs text-[var(--ink-muted)]">{match.name}</div>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
