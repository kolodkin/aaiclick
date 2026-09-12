import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App } from "./App";
import { AuthProvider } from "./components/Auth";
import { ToastProvider } from "./components/Toast";
import { isLiveConnected } from "./api/events";
import "./styles/globals.css";

// Live updates arrive over /api/v0/events (src/api/events.ts); the 2 s poll
// is only the fallback while that stream is disconnected. The function is
// re-evaluated after every fetch, and the stream invalidates all queries on
// each connect/disconnect, so the mode switches promptly in both directions.
const queryClient = new QueryClient({
  defaultOptions: { queries: { refetchInterval: () => (isLiveConnected() ? false : 2000) } },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <ToastProvider>
          <App />
        </ToastProvider>
      </AuthProvider>
    </QueryClientProvider>
  </StrictMode>,
);
