import { useQuery } from "@tanstack/react-query";
import { datasetVersionKeys } from "../constants/queryKeys";
import { datasetVersionsApi } from "../api/datasetVersion.api";
import { mapDatasetVersion, mapDatasetVersionDetail } from "../api/datasetVersion.mapper";
import type { DatasetVersion } from "../types/datasetVersion";

export const useDatasetVersion = (projectId: string, datasetId: string) =>
  useQuery({
    queryKey: datasetVersionKeys.lists(),
    queryFn: async (): Promise<DatasetVersion[]> => {
      const data = await datasetVersionsApi.getDatasetVersions(projectId, datasetId);
      return data.map(mapDatasetVersion)
    },
  });

  export const useDatasetVersionDetail = (
  projectId?: string,
  datasetId?: string,
  versionId?: string
) =>
  useQuery({
    queryKey: datasetVersionKeys.detail(projectId!, datasetId!, versionId!),
    queryFn: async () =>{
      const data= await datasetVersionsApi.getDatasetVersionById(projectId!, datasetId!, versionId!)
      return mapDatasetVersionDetail(data)
    },
    enabled: !!projectId && !!datasetId && !!versionId,
  })