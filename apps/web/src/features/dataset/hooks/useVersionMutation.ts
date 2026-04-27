import { useMutation, useQueryClient } from "@tanstack/react-query";
import { datasetVersionsApi } from "../api/datasetVersion.api";
import type { UploadDatasetVersionRequest } from "../types/datasetVersion.dto";

export const useActiveVersion = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      versionId,
    }: {
      projectId: string;
      datasetId: string,
      versionId: string,
    }) => datasetVersionsApi.activeDatasetVersion(projectId, datasetId, versionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};

export const useUploadVersionMutation = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      req,
    }: {
      projectId: string;
      datasetId: string;
      req: UploadDatasetVersionRequest;
    }) => datasetVersionsApi.uploadDatasetVersion(projectId, datasetId, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};