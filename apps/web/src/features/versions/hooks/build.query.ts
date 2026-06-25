import {
  keepPreviousData,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { buildKeys } from "../api/version.key";
import { useVersionParams } from "@/shared/hooks/useRouteParams";
import { buildApi, type BuildViewParams } from "../api/build.api";
import {
  mapBuild,
  mapKeywordClauseView,
  mapKeywordDictionaryRule,
  mapKeywordDictionaryEvent,
  type KeywordDictionaryRuleDto,
  type KeywordDictionaryEventDto,
  type KeywordDictionaryRuleRequest,
} from "../models/build";
import type { BuildJobType } from "@/shared/types/common";

// 전처리 모델 선택지 — env allowlist라 거의 변하지 않으므로 stale을 길게.
export const useLloaModelOptions = () =>
  useQuery({
    queryKey: buildKeys.lloaModelOptions(),
    queryFn: buildApi.getLloaModelOptions,
    staleTime: 5 * 60 * 1000,
  });

// 한 버전의 모델별 결과 목록 (비교 선택지) — 버전 선택 시 조회.
export const useDocGenuinenessRuns = (
  projectId: string,
  datasetId: string,
  versionId: string,
) =>
  useQuery({
    queryKey: [...buildKeys.all, "doc_genuineness_runs", projectId, datasetId, versionId],
    queryFn: () => buildApi.getDocGenuinenessRuns(projectId, datasetId, versionId),
    enabled: !!projectId && !!datasetId && !!versionId,
  });

// 진성 분류 모델 비교 (2026-06-15) — 한 버전 + 서로 다른 두 모델이 선택돼야 실행.
export const useDocGenuinenessCompare = (
  projectId: string,
  datasetId: string,
  versionId: string,
  modelA: string,
  modelB: string,
  params?: { limit?: number; offset?: number },
) =>
  useQuery({
    queryKey: [
      ...buildKeys.all,
      "doc_genuineness_compare",
      projectId,
      datasetId,
      versionId,
      modelA,
      modelB,
      params ?? {},
    ],
    queryFn: () =>
      buildApi.compareDocGenuineness(projectId, datasetId, versionId, modelA, modelB, params),
    enabled:
      !!projectId && !!datasetId && !!versionId && !!modelA && !!modelB && modelA !== modelB,
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

// 기초분석보고서 탭 — read-only 조회(report 저장 안 함). build job이 아니라 폴링 없음.
export const useBasicAnalysis = (templateId?: string) => {
  const { projectId, datasetId, versionId } = useVersionParams();
  return useQuery({
    queryKey: [...buildKeys.all, "basic_analysis", versionId, templateId ?? ""],
    queryFn: () => buildApi.getBasicAnalysis(projectId, datasetId, versionId, templateId),
    enabled: !!projectId && !!datasetId && !!versionId,
  });
};

// "절에서 추출된 키워드" 표 전용 — clause_keywords를 group=clause로 조회.
// mapBuild(키워드 중심)와 item shape이 달라 별도 hook + 매퍼를 쓴다. 서버 q 검색 +
// limit/offset 페이징.
export const useClauseKeywordClauses = (params?: BuildViewParams) => {
  const { projectId, datasetId, versionId } = useVersionParams();
  return useQuery({
    queryKey: [
      ...buildKeys.build(versionId, "clause_keywords", undefined),
      "clause-group",
      params ?? {},
    ],
    queryFn: () =>
      buildApi.getBuildVersion(projectId, datasetId, versionId, "clause_keywords", {
        ...(params ?? {}),
        group: "clause",
      }),
    select: mapKeywordClauseView,
    placeholderData: keepPreviousData,
  });
};

// ── 키워드 정제 사전 (silverone 2026-06-25) ────────────────────────────────
// 규칙은 dataset 단위. 변경 시 키워드 결과(clause_keywords) 쿼리 + 규칙/이력
// 쿼리를 invalidate해 즉시 갱신한다. 키워드 뷰 query key는
// [...buildKeys.all, versionId, "clause_keywords", ...] prefix.

export const useKeywordDictionaryRules = (includeInactive = false) => {
  const { projectId, datasetId } = useVersionParams();
  return useQuery({
    queryKey: [
      ...buildKeys.keywordDictionary(projectId, datasetId),
      includeInactive ? "all" : "active",
    ],
    queryFn: () =>
      buildApi
        .listKeywordDictionary(projectId, datasetId, includeInactive)
        .then((d: { items?: KeywordDictionaryRuleDto[] }) =>
          (d.items ?? []).map(mapKeywordDictionaryRule),
        ),
    enabled: !!projectId && !!datasetId,
  });
};

export const useKeywordDictionaryEvents = () => {
  const { projectId, datasetId } = useVersionParams();
  return useQuery({
    queryKey: buildKeys.keywordDictionaryHistory(projectId, datasetId),
    queryFn: () =>
      buildApi
        .listKeywordDictionaryHistory(projectId, datasetId)
        .then((d: { items?: KeywordDictionaryEventDto[] }) =>
          (d.items ?? []).map(mapKeywordDictionaryEvent),
        ),
    enabled: !!projectId && !!datasetId,
  });
};

// 규칙 변경 후 invalidate 대상 — 키워드 결과(버전 전체 clause_keywords) + 규칙 + 이력.
const useInvalidateKeywordDictionary = () => {
  const { projectId, datasetId, versionId } = useVersionParams();
  const qc = useQueryClient();
  return () => {
    qc.invalidateQueries({
      queryKey: [...buildKeys.all, versionId, "clause_keywords" as BuildJobType],
    });
    qc.invalidateQueries({
      queryKey: buildKeys.keywordDictionary(projectId, datasetId),
    });
    qc.invalidateQueries({
      queryKey: buildKeys.keywordDictionaryHistory(projectId, datasetId),
    });
  };
};

export const useSetKeywordDictionaryRule = () => {
  const { projectId, datasetId } = useVersionParams();
  const invalidate = useInvalidateKeywordDictionary();
  return useMutation({
    mutationFn: (req: KeywordDictionaryRuleRequest) =>
      buildApi.setKeywordDictionaryRule(projectId, datasetId, req),
    onSuccess: invalidate,
  });
};

export const useToggleKeywordDictionaryRule = () => {
  const { projectId, datasetId } = useVersionParams();
  const invalidate = useInvalidateKeywordDictionary();
  return useMutation({
    mutationFn: ({
      ruleId,
      active,
      reason,
    }: {
      ruleId: string;
      active: boolean;
      reason?: string;
    }) =>
      buildApi.setKeywordDictionaryRuleActive(
        projectId,
        datasetId,
        ruleId,
        active,
        reason,
      ),
    onSuccess: invalidate,
  });
};
