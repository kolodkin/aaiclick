import { useState } from "react";

// Per-viewer UI preference in localStorage. Storage can throw (private mode,
// blocked site data), so a failed read is "off" and a failed write is dropped.
export function readFlag(key: string): boolean {
  try {
    return localStorage.getItem(key) === "1";
  } catch {
    return false;
  }
}

export function writeFlag(key: string, on: boolean): void {
  try {
    localStorage.setItem(key, on ? "1" : "0");
  } catch {
    // The preference just won't survive a reload.
  }
}

export function useStoredFlag(key: string): [boolean, (on: boolean) => void] {
  const [on, setOn] = useState(() => readFlag(key));
  const set = (next: boolean) => {
    writeFlag(key, next);
    setOn(next);
  };
  return [on, set];
}
