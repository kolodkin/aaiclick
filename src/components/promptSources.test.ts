import { beforeEach, describe, expect, test, vi } from "vitest";
import { fetchJSON } from "../api/client";
import { matchSource, PAGE } from "./promptSources";

vi.mock("../api/client", () => ({ fetchJSON: vi.fn() }));
const fetchMock = vi.mocked(fetchJSON);

describe("matchSource", () => {
  test.each([
    ["@task ", "tasks", ""],
    ["@task load", "tasks", "load"],
    ["@job etl", "jobs:@job", "etl"],
    ["cancel ", "jobs:cancel", ""],
    ["run nightly", "registered:run", "nightly"],
    ["enable ", "registered:enable", ""],
    ["disable x", "registered:disable", "x"],
    ["@data ", "objects:@data", ""],
    ["@data persistent fo", "objects:@data persistent", "fo"],
    ["@job etl graph", "jobs:@job", "etl"],
    ["@job etl timings", "jobs:@job", "etl"],
    ["@data job ", "jobs:@data job", ""],
    ["@data job etl ", "objects:@data job etl", ""],
    ["@data job etl ord", "objects:@data job etl", "ord"],
    ["@dashboard sa", "dashboards", ""],
    ["@sandbox ", "sandbox", ""],
    ["@sandbox report", "sandbox", ""],
  ])("%s → %s / %s", (draft, key, text) => {
    const m = matchSource(draft, { sandbox: true });
    expect(m?.key).toBe(key);
    expect(m?.text).toBe(text);
  });

  test.each(["", "@jobs", "@jo", "@task", "@registered"])("%s has no source", (draft) => {
    expect(matchSource(draft, { sandbox: true })).toBeNull();
  });

  test("@sandbox needs sandbox enabled", () => {
    expect(matchSource("@sandbox ", { sandbox: false })).toBeNull();
  });
});

test("commands sharing an endpoint never share cached rows", () => {
  const keys = [
    "run ",
    "enable ",
    "disable ",
    "@job ",
    "cancel ",
    "@data job ",
    "@data ",
    "@data persistent ",
    "@data job etl ",
  ].map((d) => matchSource(d, { sandbox: false })!.key);
  expect(new Set(keys).size).toBe(keys.length);
});

describe("fetchPage", () => {
  beforeEach(() => fetchMock.mockReset());

  test("tasks narrow by name and page by offset", async () => {
    fetchMock.mockResolvedValue({ items: [] });
    await matchSource("@task load", { sandbox: false })!.fetchPage(PAGE);
    expect(fetchMock).toHaveBeenCalledWith(`/tasks?limit=${PAGE}&offset=${PAGE}&name=%25load%25`);
  });

  test("a full page has more, a short one does not", async () => {
    const task = { id: "7", name: "t", job_id: "1", status: "PENDING", created_at: "2026-01-01T00:00:00Z" };
    fetchMock.mockResolvedValueOnce({ items: Array(PAGE).fill(task) }).mockResolvedValueOnce({ items: [task] });
    const m = matchSource("@task ", { sandbox: false })!;
    expect((await m.fetchPage(0)).more).toBe(true);
    const short = await m.fetchPage(PAGE);
    expect(short.more).toBe(false);
    expect(short.items[0].insert).toBe("@task 7");
  });

  test("a full last page has no more when the total says so", async () => {
    const task = { id: "7", name: "t", job_id: "1", status: "PENDING", created_at: "2026-01-01T00:00:00Z" };
    fetchMock.mockResolvedValue({ items: Array(PAGE).fill(task), total: PAGE * 2 });
    const m = matchSource("@task ", { sandbox: false })!;
    expect((await m.fetchPage(0)).more).toBe(true);
    expect((await m.fetchPage(PAGE)).more).toBe(false);
  });

  // A name resolves to the latest run, so picking an older run's row must insert its id.
  test.each([
    ["cancel ", "cancel 9"],
    ["@job ", "@job 9"],
    ["@data job ", "@data job 9"],
  ])("%s inserts the picked run's id", async (draft, insert) => {
    const job = { id: "9", name: "etl", status: "RUNNING", created_at: "2026-01-01T00:00:00Z" };
    fetchMock.mockResolvedValue({ items: [job] });
    expect((await matchSource(draft, { sandbox: false })!.fetchPage(0)).items[0].insert).toBe(insert);
  });

  test("@data persistent narrows by the object prefix and keeps the keyword", async () => {
    fetchMock.mockResolvedValue({ items: [{ name: "orders" }] });
    const page = await matchSource("@data persistent or", { sandbox: false })!.fetchPage(0);
    expect(fetchMock).toHaveBeenCalledWith("/objects?limit=1000&prefix=or");
    expect(page.items[0].insert).toBe("@data persistent orders");
  });

  test("dashboards come in one page and narrow in the browser, not per keystroke", async () => {
    fetchMock.mockResolvedValue({ items: [{ name: "sales" }, { name: "ops" }] });
    const m = matchSource("@dashboard sa", { sandbox: false })!;
    const page = await m.fetchPage(0);
    expect(m.local).toBe("sa");
    expect(page.items.map((i) => i.insert)).toEqual(["@dashboard sales", "@dashboard ops"]);
    expect(page.more).toBe(false);
  });

  test("objects of a job keep the job scope", async () => {
    fetchMock.mockResolvedValue({ items: [{ name: "orders" }] });
    const page = await matchSource("@data job etl or", { sandbox: false })!.fetchPage(0);
    expect(fetchMock).toHaveBeenCalledWith("/objects?limit=1000&prefix=or&job=etl");
    expect(page.items[0].insert).toBe("@data job etl orders");
  });
});
