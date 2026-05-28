import { useQuery } from "@tanstack/react-query";
import { buildKeys } from "../api/version.key";
import { useVersionParams } from "@/shared/hooks/useRouteParams";
import { buildApi } from "../api/build.api";
import { mapBuild } from "../models/build";
import type { BuildJobType } from "@/shared/types/common";

export const useBuildVersion = (type: BuildJobType, jobId?: string) => {
  const { projectId, datasetId, versionId } = useVersionParams();
  return useQuery({
    queryKey: buildKeys.build(versionId, type, jobId),

    queryFn: () =>
      buildApi.getBuildVersion(projectId, datasetId, versionId, type),
    structuralSharing: false,
    refetchInterval: (query) => {
      const status = query.state.data?.status;

      return status === "queued" || status === "running" ? 5000 : false;
    },

    select: mapBuild,
  });
};
