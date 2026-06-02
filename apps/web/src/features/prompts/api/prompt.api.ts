import { apiClient } from "@/api/client";
import type { PromptOptionsResponseDto, PromptOptionsTask } from "../models/dto";

export const promptApi = {
  getOptions: (task: PromptOptionsTask) =>
    apiClient
      .get<PromptOptionsResponseDto>("/prompt_options", { params: { task } })
      .then((r) => r.data),
};
