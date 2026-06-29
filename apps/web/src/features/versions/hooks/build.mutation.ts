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

// 실행 중 build 중단(silverone 2026-06-29) — worker가 거기까지 결과 보존 후 멈춘다.
// 성공해도 즉시 멈추진 않으므로(진행 중 호출 마무리) 폴링을 그대로 두고 상태가
// cancelled로 바뀌길 기다린다. invalidate로 stage 상태도 갱신.
export const useCancelBuildJob = () => {
  const { projectId, datasetId, versionId } = useVersionParams();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ type }: { type: BuildJobType }) =>
      buildApi.cancelBuildVersion(projectId, datasetId, versionId, type),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: buildKeys.all });
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

// silverone 2026-06-11 — 절 aspect/sentiment 수동 보정. set/되돌리기 후
// clause_label view를 invalidate해 effective 값·summary를 다시 받는다.
export const useClauseLabelOverride = () => {
  const { projectId, datasetId, versionId } = useVersionParams();
  const queryClient = useQueryClient();
  const invalidate = () =>
    queryClient.invalidateQueries({
      queryKey: buildKeys.build(versionId, "clause_label"),
    });

  const set = useMutation({
    mutationFn: ({
      clauseId,
      aspect,
      sentiment,
      reason,
    }: {
      clauseId: string;
      aspect?: string;
      sentiment?: string;
      reason?: string;
    }) =>
      buildApi.setClauseLabelOverride(projectId, datasetId, versionId, clauseId, {
        aspect,
        sentiment,
        reason,
      }),
    onSuccess: invalidate,
  });

  const remove = useMutation({
    mutationFn: ({ clauseId }: { clauseId: string }) =>
      buildApi.deleteClauseLabelOverride(projectId, datasetId, versionId, clauseId),
    onSuccess: invalidate,
  });

  return { set, remove };
};
