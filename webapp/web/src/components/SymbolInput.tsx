import { useMemo, useRef, useState } from "react";

/** Symbol entry that completes rather than demands exactness.
 *
 *  Typing "relia" offers NSE:RELIANCE-EQ. Typing a bare name with no exchange
 *  offers the NSE and BSE equity forms, because that is what the prefix and
 *  suffix would have been anyway — the broker's format is not worth retyping.
 *
 *  Free text still works: a symbol the dashboard has never seen is exactly the
 *  case a fixed list would block.
 */
export function SymbolInput({
  value,
  onChange,
  known,
  className,
  autoFocus,
}: {
  value: string;
  onChange: (symbol: string) => void;
  known: string[];
  className?: string;
  autoFocus?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const blurTimer = useRef<number>();

  const suggestions = useMemo(() => {
    const text = value.trim().toUpperCase();
    if (!text) return known.slice(0, 8);

    const matches = known.filter((s) => s.includes(text));
    // A bare name is far more likely to be an equity than anything else, so
    // offer the two forms it would take rather than nothing at all.
    if (!text.includes(":") && matches.length < 8) {
      const bare = text.replace(/[^A-Z0-9&-]/g, "");
      for (const guess of [`NSE:${bare}-EQ`, `BSE:${bare}-EQ`]) {
        if (bare && !matches.includes(guess)) matches.push(guess);
      }
    }
    return matches.slice(0, 8);
  }, [value, known]);

  function choose(symbol: string) {
    onChange(symbol);
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
          // Let a click on a suggestion land before the list closes.
          blurTimer.current = window.setTimeout(() => setOpen(false), 150);
        }}
        onKeyDown={(event) => {
          if (!open || !suggestions.length) return;
          if (event.key === "ArrowDown") {
            event.preventDefault();
            setActive((i) => Math.min(i + 1, suggestions.length - 1));
          } else if (event.key === "ArrowUp") {
            event.preventDefault();
            setActive((i) => Math.max(i - 1, 0));
          } else if (event.key === "Enter" || event.key === "Tab") {
            if (suggestions[active]) {
              event.preventDefault();
              choose(suggestions[active]);
            }
          } else if (event.key === "Escape") {
            setOpen(false);
          }
        }}
        placeholder="reliance"
        className={className}
        style={{ borderColor: "var(--border)" }}
      />

      {open && suggestions.length > 0 && (
        <ul
          className="absolute z-30 mt-1 w-full overflow-hidden rounded border shadow-lg"
          style={{ background: "var(--surface)", borderColor: "var(--border)" }}
        >
          {suggestions.map((symbol, index) => (
            <li key={symbol}>
              <button
                onMouseDown={() => {
                  window.clearTimeout(blurTimer.current);
                  choose(symbol);
                }}
                onMouseEnter={() => setActive(index)}
                className={`block w-full px-3 py-1.5 text-left text-sm ${
                  index === active ? "bg-black/5 dark:bg-white/10" : ""
                }`}
              >
                {symbol}
                {!known.includes(symbol) && (
                  <span className="ml-2 text-xs text-[var(--ink-muted)]">new</span>
                )}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
