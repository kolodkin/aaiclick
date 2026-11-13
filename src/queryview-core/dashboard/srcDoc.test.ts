import { describe, expect, test } from "vitest";
import { buildSrcDoc } from "./srcDoc";

describe("buildSrcDoc", () => {
  test("exposes results and params before the HTML", () => {
    const params = [{ name: "t", kind: "value" as const, options: ["a"], value: "a" }];
    const doc = buildSrcDoc("<h1>hi</h1>", { q: { n: [1, 2] } }, params);
    expect(doc.startsWith('<script>window.queries = {"q":{"n":[1,2]}};')).toBe(true);
    expect(doc).toContain('window.params = [{"name":"t","kind":"value","options":["a"],"value":"a"}];');
    expect(doc).toContain("window.setParams = function");
    expect(doc.endsWith("</script>\n<h1>hi</h1>")).toBe(true);
  });

  test("escapes < in result data so it cannot close the prologue script", () => {
    const doc = buildSrcDoc("", { q: { s: ["</script><img onerror=x>"] } });
    expect(doc).not.toContain("</script><img");
    expect(doc).toContain("\\u003c/script>\\u003cimg");
  });
});
