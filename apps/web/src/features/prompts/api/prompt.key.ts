import type { PromptOptionsTask } from "../models";

export const promptKeys = {
  all: ["prompts"] as const,
  options: (task: PromptOptionsTask) =>
    [...promptKeys.all, "options", task] as const,
};
