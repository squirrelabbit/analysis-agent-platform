import { useQuery } from "@tanstack/react-query";
import { datasetVersionKeys } from "../constants/queryKeys";
import { datasetVersionsApi } from "../api/datasetVersion.api";
import { mapDatasetVersion } from "../api/datasetVersion.mapper";

export const useDatasetVersion = (projectId: string, datasetId: string) =>
  useQuery({
    queryKey: datasetVersionKeys.lists(),
    queryFn: async () => {
      const data = await datasetVersionsApi.getDatasetVersions(projectId, datasetId);
      return data.map(mapDatasetVersion);
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
      return mapDatasetVersion(await datasetVersionsApi.getDatasetVersionById(projectId!, datasetId!, versionId!))
    },
    enabled: !!projectId && !!datasetId && !!versionId,
  })