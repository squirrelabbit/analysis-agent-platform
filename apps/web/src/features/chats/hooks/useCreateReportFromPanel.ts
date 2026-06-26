import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { reportDocApi } from "@/features/reports/api/reportDoc.api";
import { reportKeys } from "@/features/reports/api/report.key";
import type { ReportBlock } from "@/features/reports/models";

// 패널 staging 블록을 보고서 문서로 저장한다.
//   - 새 보고서(loadedReportId 없음): POST /reports { title, blocks } (생성)
//   - 기존 보고서 불러온 경우:        PUT  /reports/{id} { title, blocks } (갱신)
// 블록은 self-contained snapshot이라 그대로 영속. 저장 후 에디터(/reports/:id)로 이동.

export interface CreateReportParams {
  staged: ReportBlock[];
  reportTitle: string;
  /** 불러온 기존 보고서 id(있으면 갱신, 없으면 생성). */
  loadedReportId: string | null;
}

export function useCreateReportFromPanel(projectId: string) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [isPending, setIsPending] = useState(false);

  const create = useCallback(
    async ({ staged, reportTitle, loadedReportId }: CreateReportParams) => {
      if (staged.length === 0 || isPending) return;
      setIsPending(true);
      try {
        const body = {
          title: reportTitle.trim() || undefined,
          blocks: staged,
        };
        const doc = loadedReportId
          ? await reportDocApi.update(projectId, loadedReportId, body)
          : await reportDocApi.create(projectId, body);
        // 단건/목록 캐시 갱신(갱신 시 에디터가 최신 blocks를 읽도록).
        queryClient.setQueryData(
          reportKeys.document(projectId, doc.report_id),
          doc,
        );
        queryClient.invalidateQueries({
          queryKey: reportKeys.documentList(projectId),
        });
        navigate(`/projects/${projectId}/reports/${doc.report_id}`);
        return doc;
      } finally {
        setIsPending(false);
      }
    },
    [projectId, isPending, navigate, queryClient],
  );

  return { create, isPending };
}
