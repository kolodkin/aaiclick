import { useCallback, useEffect, useRef, useState } from "react";

// Copy text to the clipboard and flag `copied` briefly, for a "Copied"
// confirmation. A blocked clipboard (insecure context) is a silent no-op.
export function useCopy(ms = 1500): [boolean, (text: string) => Promise<void>] {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  const copy = useCallback(
    async (text: string) => {
      try {
        await navigator.clipboard.writeText(text);
      } catch {
        return;
      }
      setCopied(true);
      clearTimeout(timer.current);
      timer.current = setTimeout(() => setCopied(false), ms);
    },
    [ms],
  );
  return [copied, copy];
}
