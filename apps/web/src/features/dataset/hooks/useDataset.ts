import { useQuery } from "@tanstack/react-query";
import { datasetKeys } from "../constants/queryKeys";
import { datasetsApi } from "../api/dataset.api";
import { mapDataset } from "../api/dataset.mapper";

export const useDataset = (projectId: string) =>
  useQuery({
    queryKey: datasetKeys.lists(),
    queryFn: async () => {
      const data = await datasetsApi.getDatasets(projectId);
      return data.map(mapDataset);
    },
  });

export const useDatasetDetail = (projectId: string, datasetId: string) =>
  useQuery({
    queryKey: datasetKeys.detail(projectId),
    queryFn: async () =>{
      return mapDataset(await datasetsApi.getDatasetById(projectId, datasetId))
    },
    enabled: !!projectId || !!datasetId,
  });