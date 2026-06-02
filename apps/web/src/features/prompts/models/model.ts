import type { PromptOptionsTask } from "./dto";

export type { PromptOptionsTask };

export interface PromptVersion {
  version: string;
  label: string;
}

export interface PromptOptions {
  task: PromptOptionsTask;
  default: string;
  versions: PromptVersion[];
}
