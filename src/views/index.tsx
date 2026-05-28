export { Home } from "./Home";
export { Jobs } from "./Jobs";
export { JobDetail } from "./JobDetail";
export { TaskDetail } from "./TaskDetail";
export function Registered({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Registered</h2>;
}
export function RunConfirm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Run {name}?</h2>;
}
export function RunForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Run {name}</h2>;
}
export function RegisterForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Register {name}</h2>;
}
export function CancelConfirm({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Cancel {refId}?</h2>;
}
export function AllGallery({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>All</h2>;
}
