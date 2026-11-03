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
    ["@data ", "objects", ""],
    ["@query ord", "objects", "ord"],
    ["@data job ", "jobs:@data job", ""],
    ["@query job et", "jobs:@query job", "et"],
    ["@data job etl ", "objects:etl", ""],
    ["@data job etl ord", "objects:etl", "ord"],
    ["@dashboard sa", "dashboards", "sa"],
    ["@sandbox ", "sandbox", ""],
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
  const keys = ["run ", "enable ", "disable ", "@job ", "cancel ", "@data job ", "@query job "].map(
    (d) => matchSource(d, { sandbox: false })!.key,
  );
  expect(new Set(keys).size).toBe(keys.length);
});

describe("fetchPage", () => {
  beforeEach(() => fetchMock.mockReset());

  test("tasks narrow by name and page by offset", async () => {
    fetchMock.mockResolvedValue({ items: [] });
    await matchSource("@task load", { sandbox: false })!.fetchPage(PAGE);
    expect(fetchMock).toHaveBeenCalledWith(`/tasks?limit=${PAGE}&offset=${PAGE}&name=load`);
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

  test("cancel inserts the job id, @job the name", async () => {
    const job = { id: "9", name: "etl", status: "RUNNING", created_at: "2026-01-01T00:00:00Z" };
    fetchMock.mockResolvedValue({ items: [job] });
    expect((await matchSource("cancel ", { sandbox: false })!.fetchPage(0)).items[0].insert).toBe("cancel 9");
    expect((await matchSource("@job ", { sandbox: false })!.fetchPage(0)).items[0].insert).toBe("@job etl");
  });

  test("dashboards filter client-side in one page", async () => {
    fetchMock.mockResolvedValue({ items: [{ name: "sales" }, { name: "ops" }] });
    const page = await matchSource("@dashboard sa", { sandbox: false })!.fetchPage(0);
    expect(page.items.map((i) => i.insert)).toEqual(["@dashboard sales"]);
    expect(page.more).toBe(false);
  });

  test("objects of a job keep the job scope", async () => {
    fetchMock.mockResolvedValue({ items: [{ name: "orders" }] });
    const page = await matchSource("@query job etl or", { sandbox: false })!.fetchPage(0);
    expect(fetchMock).toHaveBeenCalledWith("/objects?limit=1000&prefix=or&job=etl");
    expect(page.items[0].insert).toBe("@query job etl orders");
  });
});
