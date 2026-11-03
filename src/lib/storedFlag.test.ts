import { afterEach, describe, expect, test, vi } from "vitest";
import { readFlag, writeFlag } from "./storedFlag";

const store = new Map<string, string>();
const working = {
  getItem: (k: string) => store.get(k) ?? null,
  setItem: (k: string, v: string) => void store.set(k, v),
};
const blocked = {
  getItem: () => {
    throw new Error("SecurityError");
  },
  setItem: () => {
    throw new Error("SecurityError");
  },
};

afterEach(() => {
  vi.unstubAllGlobals();
  store.clear();
});

describe("stored flags", () => {
  test("round-trip", () => {
    vi.stubGlobal("localStorage", working);
    expect(readFlag("k")).toBe(false);
    writeFlag("k", true);
    expect(readFlag("k")).toBe(true);
  });

  test("blocked storage reads off and drops writes", () => {
    vi.stubGlobal("localStorage", blocked);
    expect(readFlag("k")).toBe(false);
    expect(() => writeFlag("k", true)).not.toThrow();
  });
});
