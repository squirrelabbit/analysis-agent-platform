import { useQuery } from "@tanstack/react-query";
import { datasetKeys } from "../api/dataset.key";
import { useDatasetParams, useProjectParams } from "@/shared/hooks/useRouteParams";
import { datasetApi } from "../api/dataset.api";
import { mapDataset } from "../models/mapper";

export const useDatasets = () => {
  const { projectId } = useProjectParams();
  return useQuery({
    queryKey: datasetKeys.list(projectId),
    queryFn: () => datasetApi.getDatasets(projectId),
    select: (data) => data.map(mapDataset),
  });
};

export const useDataset = () => {
  const { projectId, datasetId } = useDatasetParams();
  return useQuery({
    queryKey: datasetKeys.detail(projectId, datasetId),
    queryFn: () => datasetApi.getDataset(projectId, datasetId),
    select: mapDataset,
  });
};

