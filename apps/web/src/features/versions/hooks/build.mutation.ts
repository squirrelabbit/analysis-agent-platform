import { useVersionParams } from "@/shared/hooks/useRouteParams";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { buildApi } from "../api/build.api";
import { buildKeys, versionKeys } from "../api/version.key";
import type { BuildJobType } from "@/shared/types/common";

export const useBuildJob = <T>() => {
  const { projectId, datasetId, versionId } = useVersionParams();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({type, req}: { type: BuildJobType, req?: T }) =>
      buildApi.runBuildVersion(projectId, datasetId, versionId, type, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: buildKeys.all });
      // version 상세 stage 상태 즉시 반영 (build polling이 따라잡기 전까지)
      queryClient.invalidateQueries({
        queryKey: versionKeys.detail(projectId, datasetId, versionId),
      });
    },
  });
};
