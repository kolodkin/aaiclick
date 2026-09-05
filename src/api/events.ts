// Live updates over `GET /api/v0/events` (server-sent events).
//
// The stream carries one event kind, `changed`, with no payload: on every
// frame — and on every (re)connect, to catch up on anything missed — the
// whole React Query cache is invalidated and REST supplies fresh state.
// `EventSource` cannot send the bearer / tenant headers, so the stream is read
// through `fetch` via the same auth chokepoint as every other request.
//
// While the stream is down, `isLiveConnected()` returns false and the
// QueryClient's default `refetchInterval` (see main.tsx) falls back to 2 s
// polling, so a proxy that buffers SSE degrades to the pre-SSE behaviour
// rather than a frozen UI.
import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { openStream } from "./client";

const RECONNECT_MIN_MS = 1000;
const RECONNECT_MAX_MS = 30000;

let connected = false;

export function isLiveConnected(): boolean {
  return connected;
}

// Split the decoded byte stream into SSE frames (blank-line delimited) and
// call `onEvent` with each frame's `event:` name. Comments (`: keepalive`)
// have no event field and are dropped.
async function readFrames(body: ReadableStream<Uint8Array>, onEvent: (name: string) => void): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    buffer += decoder.decode(value, { stream: true });
    let end = buffer.indexOf("\n\n");
    while (end !== -1) {
      const frame = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      for (const line of frame.split("\n")) {
        if (line.startsWith("event:")) onEvent(line.slice(6).trim());
      }
      end = buffer.indexOf("\n\n");
    }
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

// Mount once, after auth resolves. Keeps one stream open for the lifetime of
// the app and reconnects with capped exponential backoff.
export function useLiveUpdates(): void {
  const qc = useQueryClient();
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;

    const setConnected = (value: boolean) => {
      if (connected === value) return;
      connected = value;
      // Refetch now: on connect to catch up, on disconnect so the fallback
      // interval gets re-evaluated when that fetch settles.
      void qc.invalidateQueries();
    };

    const run = async () => {
      let backoff = RECONNECT_MIN_MS;
      while (!signal.aborted) {
        try {
          const res = await openStream("/events", signal);
          // A 401 that survived the silent refresh already sent the app back
          // to the login screen; do not hammer the endpoint from there.
          if (res.status === 401) return;
          if (!res.ok || !res.body) throw new Error(`events stream: ${res.status}`);
          setConnected(true);
          backoff = RECONNECT_MIN_MS;
          await readFrames(res.body, (name) => {
            if (name === "changed") void qc.invalidateQueries();
          });
        } catch {
          // Network error or abort — fall through to the reconnect wait.
        }
        setConnected(false);
        if (signal.aborted) return;
        await sleep(backoff, signal);
        backoff = Math.min(backoff * 2, RECONNECT_MAX_MS);
      }
    };
    void run();

    return () => {
      controller.abort();
      connected = false;
    };
  }, [qc]);
}

// Null component so App can mount the stream declaratively, inside the
// QueryClientProvider and only once the user is authenticated.
export function LiveUpdates(): null {
  useLiveUpdates();
  return null;
}
