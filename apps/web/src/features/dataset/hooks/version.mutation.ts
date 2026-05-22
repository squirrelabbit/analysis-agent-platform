import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { Stage } from "../types/datasetVersion";
import { datasetVersionsApi } from "../api/datasetVersion.api";

export const useBuildJob = <T>() => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      versionId,
      stage,
      req
    }: {
      projectId: string;
      datasetId: string;
      versionId: string;
      stage: Stage,
      req?: T
    }) => datasetVersionsApi.buildJob(projectId, datasetId, versionId, stage, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};