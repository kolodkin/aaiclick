// See docs/designs/ui.md — "Prompt dropdown".
import { useState, type KeyboardEvent } from "react";
import { useSandboxConfig } from "../api/hooks";
import { useAuth } from "./Auth";
import { isCommitOnType, useCompletions } from "./commands";
import { PromptDropdown, usePromptItems } from "./PromptDropdown";
import type { SourceItem } from "./promptSources";

interface HeaderProps {
  prompt: string;
  /** Commit counter; a change resets the draft even when `prompt` is unchanged. */
  commits: number;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, commits, onPrompt }: HeaderProps) {
  const { me, signOut } = useAuth();
  const completions = useCompletions();
  const sandbox = useSandboxConfig().data?.enabled ?? false;
  // The input edits a draft; the view follows only the committed `prompt`.
  const [draft, setDraftState] = useState(prompt);
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const list = usePromptItems(draft, open, sandbox, completions);
  const { items } = list;

  // A new draft is a new list; pages loading or refetching keep the highlight.
  const setDraft = (value: string) => {
    setDraftState(value);
    setActive(-1);
  };

  // A commit from elsewhere (a click in a view, Back) replaces the draft — during
  // render, not in an effect, so the input never shows the old draft for a frame.
  const [seen, setSeen] = useState({ prompt, commits });
  if (seen.prompt !== prompt || seen.commits !== commits) {
    setSeen({ prompt, commits });
    setDraft(prompt);
    setOpen(false);
  }

  // A command that still awaits its argument (`@task `) stays a draft.
  const pick = (item: SourceItem) => {
    if (item.insert.endsWith(" ")) setDraft(item.insert);
    else onPrompt(item.insert);
  };

  const onChange = (value: string) => {
    setDraft(value);
    setOpen(true);
    if (isCommitOnType(value, completions)) onPrompt(value);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      setOpen(true);
      if (!items.length) return;
      const next = (active + (e.key === "ArrowDown" ? 1 : -1) + items.length) % items.length;
      setActive(next);
      // The scroll sentinel may sit below the fold, so arrowing onto the last row loads more itself.
      if (next === items.length - 1 && list.query.hasNextPage) void list.query.fetchNextPage();
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (open && active >= 0 && items[active]) pick(items[active]);
      else onPrompt(draft);
    } else if (e.key === "Escape") {
      setDraft(prompt);
      setOpen(false);
    }
  };

  return (
    <header>
      <button type="button" className="logo" id="header-home" onClick={() => onPrompt("")}>
        aai<span>click</span>
      </button>
      <div className="prompt-wrap">
        <input
          id="prompt"
          value={draft}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={onKeyDown}
          onFocus={() => setOpen(true)}
          onBlur={() => setOpen(false)}
          placeholder="Type a command…  try @jobs, run <registered job>, or help"
          autoComplete="off"
          spellCheck={false}
          role="combobox"
          aria-expanded={open}
          aria-controls="prompt-dropdown"
        />
        {open && <PromptDropdown list={list} active={active} onActive={setActive} onPick={pick} />}
      </div>
      {me?.username ? (
        <div className="hint user-menu">
          <span className="name-link" id="header-username" onClick={() => onPrompt("@account")}>
            {me.username}
          </span>
          <button id="header-signout" className="btn btn-sm" onClick={() => void signOut()}>
            Sign out
          </button>
        </div>
      ) : (
        <div className="hint">prompt drives the view ↑</div>
      )}
    </header>
  );
}
