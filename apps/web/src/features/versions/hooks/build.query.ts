import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { buildKeys } from "../api/version.key";
import { useVersionParams } from "@/shared/hooks/useRouteParams";
import { buildApi, type BuildViewParams } from "../api/build.api";
import { mapBuild } from "../models/build";
import type { BuildJobType } from "@/shared/types/common";

// 전처리 모델 선택지 — env allowlist라 거의 변하지 않으므로 stale을 길게.
export const useLloaModelOptions = () =>
  useQuery({
    queryKey: buildKeys.lloaModelOptions(),
    queryFn: buildApi.getLloaModelOptions,
    staleTime: 5 * 60 * 1000,
  });

// 진성 분류 모델 비교 (2026-06-15) — 두 버전 모두 선택돼야 실행.
export const useDocGenuinenessCompare = (
  projectId: string,
  datasetId: string,
  versionA: string,
  versionB: string,
  params?: { limit?: number; offset?: number },
) =>
  useQuery({
    queryKey: [
      ...buildKeys.all,
      "doc_genuineness_compare",
      projectId,
      datasetId,
      versionA,
      versionB,
      params ?? {},
    ],
    queryFn: () =>
      buildApi.compareDocGenuineness(projectId, datasetId, versionA, versionB, params),
    enabled: !!projectId && !!datasetId && !!versionA && !!versionB && versionA !== versionB,
  });

export const useBuildVersion = (
  type: BuildJobType,
  jobId?: string,
  params?: BuildViewParams,
) => {
  const { projectId, datasetId, versionId } = useVersionParams();
  return useQuery({
    // params(limit/offset/aspect/sentiment)도 키에 포함 → 변경 시 refetch.
    queryKey: [...buildKeys.build(versionId, type, jobId), params ?? {}],

    queryFn: () =>
      buildApi.getBuildVersion(projectId, datasetId, versionId, type, params),
    // 페이지/필터 변경 시 새 데이터 도착 전까지 이전 페이지 유지 → 깜빡임 방지.
    placeholderData: keepPreviousData,
    structuralSharing: false,
    refetchInterval: (query) => {
      const status = query.state.data?.status;

      return status === "queued" || status === "running" ? 5000 : false;
    },

    select: mapBuild,
  });
};
