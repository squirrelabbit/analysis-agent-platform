import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { versionApi } from "../api/version.api";
import { versionKeys } from "../api/version.key";
import type { VersionFormValues } from "../schemas/version.schema";

export const useActiveVersion = () => {
  const { projectId, datasetId } = useDatasetParams();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (versionId: string) =>
      versionApi.activeVersion(projectId, datasetId, versionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
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
        queryKey: versionKeys.detail(id),
      });
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
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};
