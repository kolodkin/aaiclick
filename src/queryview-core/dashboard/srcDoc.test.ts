import { describe, expect, test } from "vitest";
import { buildSrcDoc, type QvTheme } from "./srcDoc";

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

  test("writes the theme as :root variables ahead of the prologue", () => {
    const doc = buildSrcDoc("", {}, [], { accent: "#faff69", "accent-ink": "black" });
    expect(doc.startsWith("<style>:root{--qv-accent:#faff69;--qv-accent-ink:black;}</style>\n<script>")).toBe(true);
    expect(buildSrcDoc("", {})).toMatch(/^<script>/);
  });

  test("drops theme keys and values that could leave the style block", () => {
    const bad = { accent: "red;}</style>", "x:0}</style>": "red" } as QvTheme;
    expect(buildSrcDoc("", {}, [], bad)).toMatch(/^<script>/);
  });

  test("loads the widget bundle only for dashboards that use a tag", () => {
    expect(buildSrcDoc('<qv-param name="r"></qv-param>', {})).toContain('<script defer src="/widgets.js"></script>');
    expect(buildSrcDoc("<canvas></canvas>", {})).not.toContain("widgets.js");
  });
});
