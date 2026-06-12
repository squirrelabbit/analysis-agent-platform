import { useMutation, useQueryClient } from "@tanstack/react-query";
import { reportDocApi } from "../api/reportDoc.api";
import { reportKeys } from "../api/report.key";
import type { ReportCreateRequestDto } from "../models";

// 보고서 문서 생성. 성공 시 목록 무효화. 반환값(생성된 문서)으로 에디터 이동.
export const useCreateReport = (projectId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: ReportCreateRequestDto) =>
      reportDocApi.create(projectId, body),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: reportKeys.documentList(projectId),
      });
    },
  });
};

// 제목 변경(전체 갱신 계약이라 title만 보내면 blocks가 비워질 수 있어 주의 →
// 이름 변경은 blocks를 건드리지 않도록 호출부에서 기존 blocks와 함께 보낸다).
export const useUpdateReport = (projectId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      reportId,
      title,
      blocks,
    }: {
      reportId: string;
      title?: string;
      blocks?: unknown[];
    }) => reportDocApi.update(projectId, reportId, { title, blocks }),
    onSuccess: (data, vars) => {
      // 재조회(GET) 대신 PUT 응답을 단건 캐시에 직접 써넣는다 → 저장마다 불필요한
      // GET 1회 제거. 브레드크럼 등 저장값을 읽는 뷰는 캐시 갱신으로 반영된다.
      queryClient.setQueryData(
        reportKeys.document(projectId, vars.reportId),
        data,
      );
      // 목록은 stale 표시만(에디터 화면엔 구독자 없어 즉시 GET 없음, 목록 진입 시 최신).
      queryClient.invalidateQueries({
        queryKey: reportKeys.documentList(projectId),
      });
    },
  });
};

// 삭제. 성공 시 목록 무효화.
export const useDeleteReport = (projectId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (reportId: string) => reportDocApi.remove(projectId, reportId),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: reportKeys.documentList(projectId),
      });
    },
  });
};

// 이름 변경: PUT은 title+blocks 전체 교체라, 기존 blocks를 보존하려고
// 단건을 먼저 받아 blocks와 함께 새 title을 보낸다.
export const useRenameReport = (projectId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ reportId, title }: { reportId: string; title: string }) => {
      const src = await reportDocApi.get(projectId, reportId);
      return reportDocApi.update(projectId, reportId, {
        title,
        blocks: src.blocks,
      });
    },
    onSuccess: (_data, vars) => {
      queryClient.invalidateQueries({
        queryKey: reportKeys.documentList(projectId),
      });
      queryClient.invalidateQueries({
        queryKey: reportKeys.document(projectId, vars.reportId),
      });
    },
  });
};

// 복제: 원본 단건을 받아 blocks까지 복사해 새 문서 생성.
export const useDuplicateReport = (projectId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ reportId, title }: { reportId: string; title: string }) => {
      const src = await reportDocApi.get(projectId, reportId);
      return reportDocApi.create(projectId, { title, blocks: src.blocks });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: reportKeys.documentList(projectId),
      });
    },
  });
};
