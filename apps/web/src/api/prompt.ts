import { apiClient } from "./client";
import type {
  CreatePromptPayload,
  PromptListResponse,
  PromptResponse,
} from "@/types/dto/prompt.dto";

export const promptsApi = {
  getAll: (projectId: string) =>
    apiClient
      .get<PromptListResponse>(`/projects/${projectId}/prompts`)
      .then((r) => r.data.items),

  create: (projectId: string, payload: CreatePromptPayload) =>
    apiClient
      .post<PromptResponse>(`/projects/${projectId}/prompts`, payload)
      .then((r) => r.data),
};
