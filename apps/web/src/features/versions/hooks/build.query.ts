import { useQuery } from "@tanstack/react-query";
import { versionKeys } from "../api/version.key";
import {
  useVersionParams,
} from "@/shared/hooks/useRouteParams";
import { buildApi } from "../api/build.api";
import { mapBuild } from "../models/build";
import type { BuildJobType } from "@/shared/types/common";

export const useBuildVersion = (type: BuildJobType) => {
  const { projectId, datasetId, versionId } = useVersionParams();
   return useQuery({
    queryKey:
      versionKeys.build(
        versionId,
        type,
      ),

    queryFn: () =>
      buildApi.getBuildVersion(
        projectId,
        datasetId,
        versionId,
        type,
      ),

    refetchInterval: (query) => {
      const status =
        query.state.data?.status;

      return( status === "queued" || status === "running")
        ? 10000
        : false;
    },

    select: mapBuild
  });
}