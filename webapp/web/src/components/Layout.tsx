import { useQueryClient } from "@tanstack/react-query";
import { NavLink, Outlet, useNavigate } from "react-router-dom";

import { api } from "../lib/api";

const NAV = [
  { to: "/portfolio", label: "Portfolio" },
  { to: "/positions", label: "Positions" },
  { to: "/trades", label: "Trades" },
  { to: "/orders", label: "Orders" },
  { to: "/overview", label: "Accounts" },
];

export function Layout() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  async function signOut() {
    await api.post("/auth/logout");
    queryClient.clear();
    navigate("/login");
  }

  return (
    <div className="min-h-screen">
      <header
        className="sticky top-0 z-40 border-b bg-[var(--surface)]"
        style={{ borderColor: "var(--border)" }}
      >
        <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-x-2 gap-y-2 px-4 py-2">
          <span className="mr-4 font-semibold tracking-tight">TRADING</span>
          <nav className="flex flex-wrap gap-1">
            {NAV.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  `rounded px-3 py-1.5 text-sm font-medium transition ${
                    isActive
                      ? "bg-black/5 dark:bg-white/10"
                      : "text-[var(--ink-secondary)] hover:bg-black/5 dark:hover:bg-white/10"
                  }`
                }
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
          <button
            onClick={signOut}
            className="ml-auto text-sm text-[var(--ink-secondary)] hover:underline"
          >
            Sign out
          </button>
        </div>
      </header>
      <main className="mx-auto max-w-[1600px] p-4">
        <Outlet />
      </main>
    </div>
  );
}
