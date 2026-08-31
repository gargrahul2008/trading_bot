import { useQuery } from "@tanstack/react-query";
import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "./components/Layout";
import { Loading } from "./components/ui";
import { api, ApiError } from "./lib/api";
import { LoginPage } from "./pages/LoginPage";
import { OverviewPage } from "./pages/OverviewPage";
import { PortfolioPage } from "./pages/PortfolioPage";
import { PositionsPage } from "./pages/PositionsPage";
import { TradesPage } from "./pages/TradesPage";

function useSession() {
  const { isLoading, isError, error } = useQuery({
    queryKey: ["session"],
    queryFn: () => api.get<{ authenticated: boolean }>("/auth/me"),
    retry: false,
    staleTime: 60_000,
    // Re-checked periodically so a tab left open past the idle timeout drops to
    // the sign-in screen on its own, rather than sitting on figures it can no
    // longer refresh.
    refetchInterval: 5 * 60_000,
    refetchOnWindowFocus: true,
  });
  return {
    isLoading,
    unauthenticated: isError && error instanceof ApiError && error.status === 401,
  };
}

export function App() {
  const { isLoading, unauthenticated } = useSession();

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        element={
          isLoading ? (
            <Loading what="session" />
          ) : unauthenticated ? (
            <Navigate to="/login" replace />
          ) : (
            <Layout />
          )
        }
      >
        <Route index element={<Navigate to="/portfolio" replace />} />
        <Route path="/portfolio" element={<PortfolioPage />} />
        <Route path="/positions" element={<PositionsPage />} />
        <Route path="/trades" element={<TradesPage />} />
        <Route path="/overview" element={<OverviewPage />} />
        <Route path="*" element={<Navigate to="/portfolio" replace />} />
      </Route>
    </Routes>
  );
}
