// See docs/designs/ui.md — "Prompt dropdown".
import { useCallback, useEffect, useState, type KeyboardEvent } from "react";
import { useSandboxConfig } from "../api/hooks";
import { useAuth } from "./Auth";
import { isCommitOnType, useCompletions } from "./commands";
import { PromptDropdown } from "./PromptDropdown";
import type { SourceItem } from "./promptSources";

interface HeaderProps {
  prompt: string;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, onPrompt }: HeaderProps) {
  const { me, signOut } = useAuth();
  const completions = useCompletions();
  const sandbox = useSandboxConfig().data?.enabled ?? false;
  // The input edits a draft; the view follows only the committed `prompt`.
  const [draft, setDraft] = useState(prompt);
  const [open, setOpen] = useState(false);
  const [items, setItems] = useState<SourceItem[]>([]);
  const [active, setActive] = useState(-1);

  // A commit from elsewhere (a click in a view, Back) replaces the draft.
  useEffect(() => {
    setDraft(prompt);
    setOpen(false);
  }, [prompt]);

  const onItems = useCallback((next: SourceItem[]) => {
    setItems(next);
    setActive(-1);
  }, []);

  const commit = (value: string) => {
    setDraft(value);
    setOpen(false);
    onPrompt(value);
  };

  // A command that still awaits its argument (`@task `) stays a draft.
  const pick = (item: SourceItem) => {
    if (item.insert.endsWith(" ")) setDraft(item.insert);
    else commit(item.insert);
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
      if (items.length) setActive((a) => (a + (e.key === "ArrowDown" ? 1 : -1) + items.length) % items.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (open && active >= 0 && items[active]) pick(items[active]);
      else commit(draft);
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
        {open && (
          <PromptDropdown
            draft={draft}
            sandbox={sandbox}
            completions={completions}
            active={active}
            onActive={setActive}
            onPick={pick}
            onItems={onItems}
          />
        )}
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
