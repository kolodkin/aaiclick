// Log lines are colored by the words in them; there is no stored severity.
export type LogTone = "error" | "warning" | "debug" | "plain";

const TONES: readonly [RegExp, LogTone][] = [
  [/error|exception|traceback|fatal|critical/i, "error"],
  [/warn/i, "warning"],
  [/debug/i, "debug"],
];

export function logTone(text: string): LogTone {
  return TONES.find(([re]) => re.test(text))?.[1] ?? "plain";
}
