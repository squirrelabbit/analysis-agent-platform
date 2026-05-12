import { useQuery } from "@tanstack/react-query";
import { promptKeys } from "../constants/queryKeys";
import { promptsApi } from "../api/prompt.api";
import type { PromptOperation } from "../types/prompt";
import { mapPrompt } from "../api/prompt.mapper";

export const usePrompts = (operation?: PromptOperation) =>
  useQuery({
    queryKey: promptKeys.lists(),
    queryFn: async () => {
      const data = await promptsApi.getPrompts(operation);
      return data.map(mapPrompt);
    },
  });
