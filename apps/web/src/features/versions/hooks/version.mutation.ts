import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { versionApi } from "../api/version.api";
import { versionKeys } from "../api/version.key";
import { datasetKeys } from "@/features/datasets/api/dataset.key";
import type { VersionFormValues } from "../schemas/version.schema";

export const useActiveVersion = () => {
  const { projectId, datasetId } = useDatasetParams();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (versionId: string) =>
      versionApi.activeVersion(projectId, datasetId, versionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: versionKeys.all });
      // dataset 카드의 active version 표시 갱신
      queryClient.invalidateQueries({ queryKey: datasetKeys.lists() });
    },
  });
};

export const useDeleteVersion = () => {
  const { projectId, datasetId } = useDatasetParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (versionId: string) =>
      versionApi.deleteVersion(projectId, datasetId, versionId),
    onSuccess: (_, id) => {
      queryClient.removeQueries({
        queryKey: versionKeys.detail(projectId, datasetId, id),
      });
      queryClient.invalidateQueries({
        queryKey: versionKeys.list(projectId, datasetId),
      });
      // 삭제한 버전이 active였을 수 있으므로 dataset 카드도 갱신
      queryClient.invalidateQueries({ queryKey: datasetKeys.lists() });
    },
  });
};

export const useCreateVersion = () => {
  const { projectId, datasetId } = useDatasetParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (req: VersionFormValues) => {
      const { file, dataType, activateOnCreate } = req;
      const formData = new FormData();

      formData.append("file", file);
      formData.append("data_type", dataType);
      formData.append("activate_on_create", activateOnCreate.toString());
      return versionApi.createVersion(projectId, datasetId, formData);
    },
    onSuccess: (_, req) => {
      queryClient.invalidateQueries({ queryKey: versionKeys.all });
      // activate_on_create=true면 dataset 카드의 active version이 바뀜
      if (req.activateOnCreate) {
        queryClient.invalidateQueries({ queryKey: datasetKeys.lists() });
      }
    },
  });
};
