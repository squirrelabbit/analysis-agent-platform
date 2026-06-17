import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { DatasetFormValues, DatasetMeta } from "../schemas/dataset";
import { datasetApi } from "../api/dataset.api";
import {
  mapDatasetFormToRequest,
  mapDatasetMetadataRequest,
} from "../models/mapper";
import { datasetKeys } from "../api/dataset.key";
import { projectKeys } from "@/features/projects/api/project.keys";

export const useCreateDataset = () => {
  const { projectId } = useProjectParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (req: DatasetFormValues) =>
      datasetApi.createDataset(projectId, mapDatasetFormToRequest(req)),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: datasetKeys.all });
      // 프로젝트 카드의 dataset_count 갱신
      queryClient.invalidateQueries({ queryKey: projectKeys.lists() });
    },
  });
};

export const useEditDatasetInfo = () => {
  const { projectId } = useProjectParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      datasetId,
      name,
      description,
    }: {
      datasetId: string;
      name?: string;
      description?: string;
    }) => datasetApi.updateInfo(projectId, datasetId, { name, description }),
    onSuccess: (_, { datasetId }) => {
      queryClient.invalidateQueries({
        queryKey: datasetKeys.detail(projectId, datasetId),
      });
      queryClient.invalidateQueries({ queryKey: datasetKeys.lists() });
    },
  });
};

export const useEditMetadata = () => {
  const { projectId } = useProjectParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ req, datasetId }: { req: DatasetMeta; datasetId: string }) =>
      datasetApi.patchMetadata(projectId, datasetId, {
        metadata: mapDatasetMetadataRequest(req),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: datasetKeys.all });
    },
  });
};

export const useDeleteDataset = () => {
  const { projectId } = useProjectParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (datasetId: string) =>
      datasetApi.deleteDataset(projectId, datasetId),
    onSuccess: (_, datasetId) => {
      queryClient.removeQueries({
        queryKey: datasetKeys.detail(projectId, datasetId),
      });
      queryClient.invalidateQueries({
        queryKey: datasetKeys.lists(),
      });
      // 프로젝트 카드의 dataset_count 갱신
      queryClient.invalidateQueries({ queryKey: projectKeys.lists() });
    },
  });
};
