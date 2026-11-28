import { expect, test, vi } from "vitest";
import { render } from "vitest-browser-react";
import { PromptDropdown } from "./PromptDropdown";
import type { SourceItem } from "./promptSources";

type List = Parameters<typeof PromptDropdown>[0]["list"];

const items: SourceItem[] = [
  { id: "@jobs", label: "@jobs", insert: "@jobs" },
  { id: "@data", label: "@data", insert: "@data" },
];

const idleQuery = {
  hasNextPage: false,
  isFetchingNextPage: false,
  fetchNextPage: vi.fn(),
  isLoading: false,
  isError: false,
  isSuccess: true,
} as unknown as List["query"];

test("picking a row hands it over without blurring the prompt", async () => {
  const onPick = vi.fn();
  const onActive = vi.fn();
  const screen = render(
    <>
      <input aria-label="prompt" autoFocus />
      <PromptDropdown list={{ source: null, items, query: idleQuery }} active={0} onActive={onActive} onPick={onPick} />
    </>,
  );

  await screen.getByRole("option", { name: "@data" }).click();

  expect(onActive).toHaveBeenCalledWith(1);
  expect(onPick).toHaveBeenCalledWith(items[1]);
  await expect.element(screen.getByLabelText("prompt")).toHaveFocus();
});
