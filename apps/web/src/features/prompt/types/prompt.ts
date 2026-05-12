export type PromptOperation =
  | "prepare"
  | "prepare_batch"
  | "sentiment"
  | "sentiment_batch";

export type PromptStatus = "active" | "ready" | "deprecated";

export interface Prompt {
  id: string;
  version: string;
  operation: PromptOperation;
  title: string;
  status: string;
  summary: string;
  content: string;
  contentHash: string;
  createdAt: string;
  updatedAt: string;
}
