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

// silverone 2026-06-11 — 진성 라벨 수동 보정. set/되돌리기 후 doc_genuineness
// view를 invalidate해 effective label·summary를 다시 받는다.
export const useGenuinenessOverride = () => {
  const { projectId, datasetId, versionId } = useVersionParams();
  const queryClient = useQueryClient();
  const invalidate = () =>
    queryClient.invalidateQueries({
      queryKey: buildKeys.build(versionId, "doc_genuineness"),
    });

  const set = useMutation({
    mutationFn: ({
      docId,
      genuineness,
      reason,
    }: {
      docId: string;
      genuineness: string;
      reason?: string;
    }) =>
      buildApi.setGenuinenessOverride(projectId, datasetId, versionId, docId, {
        genuineness,
        reason,
      }),
    onSuccess: invalidate,
  });

  const remove = useMutation({
    mutationFn: ({ docId }: { docId: string }) =>
      buildApi.deleteGenuinenessOverride(projectId, datasetId, versionId, docId),
    onSuccess: invalidate,
  });

  return { set, remove };
};
