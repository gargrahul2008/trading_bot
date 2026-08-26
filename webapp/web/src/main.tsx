import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import { App } from "./App";
import "./index.css";
import { PrivacyProvider } from "./lib/privacy";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Every figure carries its own age from the agent, so a retry storm buys
      // nothing — one clean failure and the staleness marker tells the truth.
      retry: false,
      refetchOnWindowFocus: true,
    },
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <PrivacyProvider>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </PrivacyProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
