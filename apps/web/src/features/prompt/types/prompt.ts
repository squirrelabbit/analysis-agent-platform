export type Operation =
  | "prepare"
  | "prepare_batch"
  | "sentiment"
  | "sentiment_batch";

export interface Prompt {
  id: string;
  version: string;
  operation: string;
  title: string;
  status: string;
  summary: string;
  content: string;
  contentHash: string;
  createdAt: string;
  updatedAt: string;
}
