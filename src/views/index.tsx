export { Home } from "./Home";
export { Jobs } from "./Jobs";
export { JobDetail } from "./JobDetail";
export { TaskDetail } from "./TaskDetail";
export { Registered } from "./Registered";
export { RunConfirm } from "./RunConfirm";
export { RunForm } from "./RunForm";
export { RegisterForm } from "./RegisterForm";
export { CancelConfirm } from "./CancelConfirm";
export function AllGallery({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>All</h2>;
}
