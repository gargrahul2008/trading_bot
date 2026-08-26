import { createContext, useCallback, useContext, useMemo, useState } from "react";
import type { ReactNode } from "react";

/** One switch that blurs every figure on screen.
 *
 *  This is a real account book. Screen-sharing, a photo, someone walking past —
 *  all of them are reasons to hide the numbers without losing the layout.
 */
const PrivacyContext = createContext<{
  hidden: boolean;
  toggle: () => void;
} | null>(null);

const STORAGE_KEY = "dashboard.privacy";

export function PrivacyProvider({ children }: { children: ReactNode }) {
  const [hidden, setHidden] = useState(() => {
    try {
      return localStorage.getItem(STORAGE_KEY) === "1";
    } catch {
      return false;
    }
  });

  const toggle = useCallback(() => {
    setHidden((current) => {
      const next = !current;
      try {
        localStorage.setItem(STORAGE_KEY, next ? "1" : "0");
      } catch {
        /* private window, or storage blocked */
      }
      return next;
    });
  }, []);

  const value = useMemo(() => ({ hidden, toggle }), [hidden, toggle]);
  return <PrivacyContext.Provider value={value}>{children}</PrivacyContext.Provider>;
}

export function usePrivacy() {
  const context = useContext(PrivacyContext);
  if (!context) throw new Error("usePrivacy outside PrivacyProvider");
  return context;
}

/** Wraps a figure so it blurs when privacy is on, keeping its footprint. */
export function Money({ children }: { children: ReactNode }) {
  const { hidden } = usePrivacy();
  return <span className={hidden ? "masked" : undefined}>{children}</span>;
}
