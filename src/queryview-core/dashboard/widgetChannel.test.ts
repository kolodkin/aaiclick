import { describe, expect, it } from "vitest";

import {
  parseFieldsAttr,
  parseOrderByAttr,
  parseWidgetRequest,
  rejectedWidgetReply,
  widgetQueryNames,
} from "./widgetChannel";

const good = {
  type: "widget-query",
  id: "a",
  query: "sales",
  limit: 100,
  offset: 0,
  order_by: [{ name: "day", dir: "DESC" }],
};

describe("parseWidgetRequest", () => {
  it("accepts a well-formed widget-query", () => {
    expect(parseWidgetRequest(good)).toEqual({
      type: "widget-query",
      id: "a",
      query: "sales",
      limit: 100,
      offset: 0,
      orderBy: [{ name: "day", dir: "DESC" }],
    });
  });

  it("accepts describe and csv", () => {
    expect(parseWidgetRequest({ type: "widget-describe", id: "b", query: "sales" })).toEqual({
      type: "widget-describe",
      id: "b",
      query: "sales",
    });
    expect(parseWidgetRequest({ ...good, type: "widget-csv", id: undefined })).toEqual({
      type: "widget-csv",
      query: "sales",
      limit: 100,
      offset: 0,
      orderBy: [{ name: "day", dir: "DESC" }],
    });
  });

  it.each([
    { limit: "x" },
    { limit: 99999 },
    { limit: 0 },
    { offset: -1 },
    { offset: 1.5 },
    { order_by: [{ name: "day", dir: "; DROP" }] },
    { order_by: "day" },
    { query: 7 },
    { id: undefined },
    { sql: "select 1", query: undefined },
  ])("rejects %o", (bad) => {
    expect(parseWidgetRequest({ ...good, ...bad })).toBeNull();
  });

  it("ignores other messages", () => {
    expect(parseWidgetRequest({ type: "set-params", values: {} })).toBeNull();
    expect(parseWidgetRequest(null)).toBeNull();
  });
});

describe("widgetQueryNames", () => {
  it("finds the queries widgets own", () => {
    const html = `<qv-select-view query="sales"></qv-select-view><p>x</p>
      <qv-select-view limit="5" query='top'></qv-select-view><qv-select-view query="sales">`;
    expect(widgetQueryNames(html)).toEqual(["sales", "top"]);
    expect(widgetQueryNames('<canvas id="c"></canvas>')).toEqual([]);
  });

  it("reads the attribute however it is written, and only that attribute", () => {
    expect(widgetQueryNames("<qv-select-view query=sales>")).toEqual(["sales"]);
    expect(widgetQueryNames('<QV-SELECT-VIEW Query = "sales">')).toEqual(["sales"]);
    expect(widgetQueryNames('<qv-select-view data-query="totals" query="main">')).toEqual(["main"]);
  });
});

describe("presentation attributes", () => {
  it("parses order-by and fields", () => {
    expect(parseOrderByAttr("day desc, name")).toEqual([
      { name: "day", dir: "DESC" },
      { name: "name", dir: "ASC" },
    ]);
    expect(parseOrderByAttr(null)).toEqual([]);
    expect(parseFieldsAttr(" day, revenue ")).toEqual(["day", "revenue"]);
    expect(parseFieldsAttr("")).toEqual([]);
  });
});

describe("rejectedWidgetReply", () => {
  it("answers a malformed widget request so the widget is not left waiting", () => {
    expect(rejectedWidgetReply({ ...good, limit: 2.5 })).toEqual({
      type: "widget-result",
      id: "a",
      ok: false,
      message: "Bad request",
    });
    expect(rejectedWidgetReply({ type: "widget-describe", id: "b" })).toEqual({
      type: "widget-fields",
      id: "b",
      ok: false,
      message: "Bad request",
    });
  });

  it("has no answer for other messages", () => {
    expect(rejectedWidgetReply({ type: "set-params", id: "a" })).toBeNull();
    expect(rejectedWidgetReply({ type: "widget-query" })).toBeNull();
    expect(rejectedWidgetReply(null)).toBeNull();
  });
});
