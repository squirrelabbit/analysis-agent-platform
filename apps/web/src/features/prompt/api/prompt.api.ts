import { apiClient } from "@/api/client";
import type { PromptCatalogResponse, PromptPayload, PromptResponse } from "../types/prompt.dto";

export const promptsApi = {
  getPromptCatalog: () =>
    apiClient.get<PromptCatalogResponse>(`/prompt_catalog`).then((r) => r.data),

  getPromptById: (id: string) =>
    apiClient.get<PromptResponse>(`/prompts/${id}`).then((r) => r.data),

  createPrompt: (req: PromptPayload) =>
    apiClient.post<PromptResponse>(`/projects`, req).then((r) => r.data),

  updatePrompt: (id: string, req: PromptPayload) =>
    apiClient.patch<PromptResponse>(`/projects/${id}`, req).then((r) => r.data),

  deletePrompt: (id: string) =>
    apiClient.delete<void>(`/prompts/${id}`).then((r) => r.data),
};
