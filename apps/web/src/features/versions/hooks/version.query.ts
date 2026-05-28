import { useQuery } from "@tanstack/react-query";
import { versionKeys } from "../api/version.key";
import { versionApi } from "../api/version.api";
import {
  useDatasetParams,
  useVersionParams,
} from "@/shared/hooks/useRouteParams";
import { mapVersion, mapVersionDetail } from "../models/version";

export const useVersions = () => {
  const { projectId, datasetId } = useDatasetParams();
  return useQuery({
    queryKey: versionKeys.list(projectId, datasetId),
    queryFn: () => versionApi.getVersions(projectId, datasetId),
    select: (data) => data.map(mapVersion),
  });
};

export const useVersion = () => {
  const { projectId, datasetId, versionId } = useVersionParams();
  return useQuery({
    queryKey: versionKeys.detail(projectId, datasetId, versionId),
    queryFn: () => versionApi.getVersion(projectId, datasetId, versionId),
    enabled: !!versionId,
    select: mapVersionDetail,
  });
};
