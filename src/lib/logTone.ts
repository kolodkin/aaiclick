// Log lines are colored by the words in them, not by a stored severity:
// a line naming an error is red, a warning yellow, a debug line dim.
export type LogTone = "error" | "warning" | "debug" | "plain";

const TONES: readonly [RegExp, LogTone][] = [
  [/error|exception|traceback|fatal|critical/i, "error"],
  [/warn/i, "warning"],
  [/debug/i, "debug"],
];

export function logTone(text: string): LogTone {
  return TONES.find(([re]) => re.test(text))?.[1] ?? "plain";
}
