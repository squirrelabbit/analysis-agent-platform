import { useVersionParams } from "@/shared/hooks/useRouteParams";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { buildApi } from "../api/build.api";
import type { BuildJobType } from "@/shared/types/common";

export const useBuildJob = <T>() => {
  const { projectId, datasetId, versionId } = useVersionParams();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({type, req}: { type: BuildJobType, req?: T }) =>
      buildApi.runBuildVersion(projectId, datasetId, versionId, type, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["builds"] });
    },
  });
};