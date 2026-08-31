import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { Card } from "../components/ui";
import { api, ApiError } from "../lib/api";

export function LoginPage() {
  const [password, setPassword] = useState("");
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const signIn = useMutation({
    mutationFn: () => api.post("/auth/login", { password }),
    onSuccess: async () => {
      // Refetch the session before leaving, so the guarded route sees an
      // authenticated session rather than the cached 401 that sent us here and
      // bouncing straight back to this screen.
      await queryClient.invalidateQueries();
      // "/" rather than a named page: the index route decides where the app
      // lands, so adding or reordering pages does not leave sign-in pointing at
      // yesterday's landing screen.
      navigate("/", { replace: true });
    },
  });

  const message =
    signIn.error instanceof ApiError ? signIn.error.message : signIn.error ? "Sign-in failed" : null;

  return (
    <div className="flex min-h-screen items-center justify-center p-4">
      <Card className="w-full max-w-sm p-6">
        <h1 className="text-lg font-semibold">Trading Dashboard</h1>
        <p className="mt-1 text-sm text-[var(--ink-secondary)]">
          Sign in to see every account.
        </p>
        <form
          className="mt-5"
          onSubmit={(event) => {
            event.preventDefault();
            signIn.mutate();
          }}
        >
          <label className="block text-sm font-medium" htmlFor="password">
            Password
          </label>
          <input
            id="password"
            type="password"
            autoFocus
            autoComplete="current-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            className="mt-1 w-full rounded border bg-transparent px-3 py-2 text-sm"
            style={{ borderColor: "var(--border)" }}
          />
          {message && (
            <p className="mt-2 text-sm" style={{ color: "var(--status-critical)" }}>
              {message}
            </p>
          )}
          <button
            type="submit"
            disabled={signIn.isPending || !password}
            className="mt-4 w-full rounded px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
            style={{ background: "var(--accent)" }}
          >
            {signIn.isPending ? "Signing in…" : "Sign in"}
          </button>
        </form>
      </Card>
    </div>
  );
}
