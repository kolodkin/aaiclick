export { Home } from "./Home";
export { Jobs } from "./Jobs";
export { JobDetail } from "./JobDetail";
export { TaskDetail } from "./TaskDetail";
export { Registered } from "./Registered";
export { RunConfirm } from "./RunConfirm";
export { RunForm } from "./RunForm";
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
