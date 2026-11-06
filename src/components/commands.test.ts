import { expect, test } from "vitest";
import { parsePrompt } from "../prompt";
import { ALL_COMMANDS, isCommitOnType } from "./commands";

// Guards the registry against drifting from parsePrompt: every prompt it emits must route.
// A trailing space in `complete` awaits an argument, so fill one in.
const prompts = ALL_COMMANDS.flatMap((c) => [c.cmd, c.complete?.replace(/ $/, " x")].filter((p) => p !== undefined));

test.each(prompts)("%s parses", (prompt) => {
  expect(parsePrompt(prompt).kind).not.toBe("unknown");
});

test.each([
  ["@jobs", true],
  ["help", true],
  ["@task ", false],
  ["@jo", false],
  ["@task 12", false],
])("isCommitOnType(%s) = %s", (draft, expected) => {
  const completions = [
    { text: "help", disabled: false },
    { text: "@jobs", disabled: false },
    { text: "@task ", disabled: false },
  ];
  expect(isCommitOnType(draft, completions)).toBe(expected);
});
