// The frame's side of the widget channel: ask the host (the dashboard page) to
// run one of the dashboard's queries. Values only; see ../dashboard/widgetChannel.ts.

import type { WidgetReply } from "../dashboard/widgetChannel";

let seq = 0;

// Post a request and resolve with the host's reply to it (matched by id).
export function requestHost<R extends WidgetReply>(msg: Record<string, unknown>): Promise<R> {
  const id = `w${++seq}`;
  return new Promise((resolve) => {
    const onMessage = (e: MessageEvent) => {
      if (e.source !== window.parent || e.data?.id !== id) return;
      window.removeEventListener("message", onMessage);
      resolve(e.data as R);
    };
    window.addEventListener("message", onMessage);
    window.parent.postMessage({ ...msg, id }, "*");
  });
}

// Fire-and-forget (the CSV download happens in the host).
export function tellHost(msg: Record<string, unknown>): void {
  window.parent.postMessage(msg, "*");
}

// The host re-ran the dashboard: a param changed (every query), or Execute ran
// the edited ones, named in `queries`.
export function onParamsResults(fn: (queries: string[] | null) => void): () => void {
  const onMessage = (e: MessageEvent) => {
    if (e.source !== window.parent || e.data?.type !== "params-results") return;
    fn(Array.isArray(e.data.queries) ? e.data.queries : null);
  };
  window.addEventListener("message", onMessage);
  return () => window.removeEventListener("message", onMessage);
}
