import { useEffect, useRef } from "react";

// Close an open popover on a pointer press outside the returned ref, or on
// Escape. Put that ref on the wrapper holding *both* the trigger and the panel,
// so pressing the trigger while open doesn't close here and immediately reopen
// in the trigger's own click handler.
export function useDismiss<T extends HTMLElement>(open: boolean, close: () => void) {
  const ref = useRef<T>(null);
  // The latest close, so callers can pass an inline arrow without the listeners
  // re-subscribing on every render while open.
  const closeRef = useRef(close);
  useEffect(() => {
    closeRef.current = close;
  });
  useEffect(() => {
    if (!open) return;
    // pointerdown, not click: it fires before focus moves or the click lands,
    // so the panel is gone by the time the outside control reacts.
    const onPointerDown = (e: PointerEvent) => {
      const el = ref.current;
      if (el && !el.contains(e.target as Node)) closeRef.current();
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeRef.current();
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);
  return ref;
}
