import { describe, expect, it } from "vitest";

import { compileCellTemplate } from "./cellTemplate";

const renderCellTemplate = (t: string, raw: string, row: string[], columns: string[]) =>
  compileCellTemplate(t)(raw, row, columns);

const cols = ["id", "name"];

describe("renderCellTemplate", () => {
  it("escapes substituted values as HTML", () => {
    expect(renderCellTemplate("<strong>{cell}</strong>", "<b>&", ["1", "<b>&"], cols)).toBe(
      "<strong>&lt;b&gt;&amp;</strong>",
    );
  });

  it("URL-encodes inside href and src", () => {
    expect(
      renderCellTemplate(
        '<a href="https://example.internal/i/{row.id}?q={cell}">{cell}</a>',
        "a b",
        ["1/2", "a b"],
        cols,
      ),
    ).toBe('<a href="https://example.internal/i/1%2F2?q=a%20b">a b</a>');
  });

  it("is single pass and keeps unknown columns", () => {
    expect(renderCellTemplate("{cell} {row.nope}", "{row.id}", ["1", "{row.id}"], cols)).toBe("{row.id} {row.nope}");
  });

  it.each(['HREF="{cell}"', "href={cell}", "href = '{cell}'", 'SRC="{cell}"'])(
    "URL-encodes %s however the attribute is written",
    (attr) => {
      expect(renderCellTemplate(`<a ${attr}>x</a>`, "javascript:alert(1)", ["1", ""], cols)).toContain(
        "javascript%3Aalert(1)",
      );
    },
  );
});
