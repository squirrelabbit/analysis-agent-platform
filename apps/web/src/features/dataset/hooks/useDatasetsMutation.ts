import { useMutation, useQueryClient } from "@tanstack/react-query";
import { datasetsApi } from "../api/dataset.api";
import type { CreateDatasetRequest } from "../types/dataset.dto";

export const useCreateDatasetMutation = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      req,
    }: {
      projectId: string;
      req: CreateDatasetRequest;
    }) => datasetsApi.createDataset(projectId, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["datasets"] });
    },
  });
};
