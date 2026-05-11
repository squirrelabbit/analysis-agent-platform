import { apiClient } from "@/api/client";
import type { PromptListResponse, PromptPayload, PromptResponse } from "../types/prompt.dto";
import type { Operation } from "../types/prompt";

export const promptsApi = {
  getPrompts: (operation?: Operation) =>
    apiClient.get<PromptListResponse>(`/prompts`, {params: {operation: operation}}).then((r) => r.data.items),

  createPrompt: (project_id: string, req: PromptPayload) =>
    apiClient.post<PromptResponse>(`/projects/${project_id}/prompts`, req).then((r) => r.data),
}