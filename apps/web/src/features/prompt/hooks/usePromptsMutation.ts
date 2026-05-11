import { useMutation, useQueryClient } from "@tanstack/react-query";
import { promptsApi } from "../api/prompt.api";
import type { PromptPayload } from "../types/prompt.dto";

export const useCreatePromptMutation = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      req,
    }: {
      projectId: string;
      req: PromptPayload;
    }) => promptsApi.createPrompt(projectId, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["prompts"] });
    },
  });
};
