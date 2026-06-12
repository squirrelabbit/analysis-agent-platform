import { useQuery } from "@tanstack/react-query";
import { reportDocApi } from "../api/reportDoc.api";
import { reportKeys } from "../api/report.key";
import { mapReport, mapReportList } from "../models";

// 보고서 문서 목록(project 스코프, 최신순).
export const useReports = (projectId: string) =>
  useQuery({
    queryKey: reportKeys.documentList(projectId),
    queryFn: () => reportDocApi.list(projectId),
    enabled: !!projectId,
    select: mapReportList,
  });

// 보고서 문서 단건(blocks 포함) — 에디터 hydrate용.
// 에디터가 자동저장으로 이 문서를 소유한다(저장 시 setQueryData로 캐시도 갱신).
// 따라서 포커스/마운트 시 자동 재조회를 끈다 — 안 그러면 저장 직후/창 전환마다
// 불필요한 GET이 계속 나간다. 최초 로드 1회만 fetch.
export const useReport = (projectId: string, reportId?: string) =>
  useQuery({
    queryKey: reportKeys.document(projectId, reportId ?? ""),
    queryFn: () => reportDocApi.get(projectId, reportId as string),
    enabled: !!projectId && !!reportId,
    select: mapReport,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnMount: false,
  });
