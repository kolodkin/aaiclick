// Registers a React component as a custom element for the dashboard frame.
// Each element renders in its own shadow root with the kernel stylesheet, so
// the agent's CSS and ours cannot touch each other.

import type { ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";

import css from "../kernel.css?inline";

// Tailwind's --tw-* defaults and the --qv-* theme tokens are @property rules,
// which a browser ignores inside a shadow root: register them once on the
// document instead. They only declare custom properties, so the agent's page
// is unaffected.
function registerProperties(): void {
  if (document.getElementById("qv-properties")) return;
  const style = document.createElement("style");
  style.id = "qv-properties";
  style.textContent = (css.match(/@property[^{]+\{[^}]*\}/g) ?? []).join("\n");
  document.head.append(style);
}

// One parsed copy of the stylesheet, adopted by every widget's shadow root.
let shared: CSSStyleSheet | null = null;
function sheets(display: string): CSSStyleSheet[] {
  if (!shared) {
    registerProperties();
    shared = new CSSStyleSheet();
    shared.replaceSync(css);
  }
  const host = new CSSStyleSheet();
  host.replaceSync(`:host{display:${display};color-scheme:dark}`);
  return [shared, host];
}

export function defineWidget(
  tag: string,
  render: (host: HTMLElement) => ReactNode,
  display: "block" | "inline-block" = "block",
): void {
  // The bundle is deferred (see ../dashboard/srcDoc.ts), so a tag in the HTML
  // upgrades with its <template> children already parsed.
  class Widget extends HTMLElement {
    private root: Root | null = null;
    connectedCallback() {
      if (this.root) return;
      // Re-attached after a removal: the shadow root is still there.
      const shadow = this.shadowRoot ?? this.attachShadow({ mode: "open" });
      shadow.adoptedStyleSheets = sheets(display);
      const el = document.createElement("div");
      shadow.replaceChildren(el);
      this.root = createRoot(el);
      this.root.render(render(this));
    }
    // A page that re-renders its HTML must not leave the old widget querying.
    disconnectedCallback() {
      this.root?.unmount();
      this.root = null;
    }
  }
  customElements.define(tag, Widget);
}
