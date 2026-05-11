import { useQuery } from "@tanstack/react-query";
import { promptKeys } from "../constants/queryKeys";
import { promptsApi } from "../api/prompt.api";
import type { Operation } from "../types/prompt";
import { mapPrompt } from "../api/prompt.mapper";

export const usePrompts = (operation?: Operation) =>
  useQuery({
    queryKey: promptKeys.lists(),
    queryFn: async () => {
      const data = await promptsApi.getPrompts(operation);
      return data.map(mapPrompt);
    },
  });
