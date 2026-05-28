import type { ChatRole } from "./dto";

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: string;
}

export interface AnalyzeResult {
  threadId: string;
  assistantMessage?: ChatMessage;
  errorMessage?: string | null;
}
