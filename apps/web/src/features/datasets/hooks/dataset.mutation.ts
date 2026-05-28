import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { DatasetFormValues, DatasetMeta } from "../schemas/dataset";
import { datasetApi } from "../api/dataset.api";
import {
  mapDatasetFormToRequest,
  mapMetadataRequest,
} from "../models/mapper";
import { datasetKeys } from "../api/dataset.key";

export const useCreateDataset = () => {
  const { projectId } = useProjectParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (req: DatasetFormValues) =>
      datasetApi.createDataset(projectId, mapDatasetFormToRequest(req)),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: datasetKeys.all });
    },
  });
};

export const useEditMetadata = () => {
  const { projectId } = useProjectParams();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ req, datasetId }: { req: DatasetMeta; datasetId: string }) =>
      datasetApi.patchMetadata(projectId, datasetId, {
        metadata: { doc_genuineness: mapMetadataRequest(req) },
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
    },
  });
};
