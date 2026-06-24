import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { createClientId } from "@/shared/utils/id";
import { reportDocApi } from "@/features/reports/api/reportDoc.api";
import { reportKeys } from "@/features/reports/api/report.key";
import type { ReportBlock } from "@/features/reports/models";
import { chatApi } from "../api/chat.api";
import type { ChatMessage } from "../models";
import type { PanelCardState } from "./useReportPanel";

// 패널에 모은 결과 카드들을 실제 보고서 문서로 만든다.
//   1) 각 결과를 보관함(saved_results)에 저장해 result_id 확보(블록 libId가 참조).
//   2) 편집한 제목(override)·메모(interp)·순서를 담아 보고서 문서 생성.
//   3) 생성된 문서 에디터(/reports/:id)로 이동.
// 블록 계약은 보고서 에디터(reports/models/editor.ts ReportBlock)와 동일하게 맞춘다.

function hasDetail(msg: ChatMessage): boolean {
  return (!!msg.metric || !!msg.evidence || !!msg.chart) && !!msg.display;
}

export interface CreateReportParams {
  staged: string[];
  reportTitle: string;
  /** runId별 출처 스레드 id(여러 스레드 결과 집계 시 카드마다 다를 수 있다). */
  threadOf: (runId: string) => string | undefined;
  messageOf: (runId: string) => ChatMessage | undefined;
  cardStateOf: (runId: string) => PanelCardState;
}

export function useCreateReportFromPanel(projectId: string) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [isPending, setIsPending] = useState(false);

  const create = useCallback(
    async ({
      staged,
      reportTitle,
      threadOf,
      messageOf,
      cardStateOf,
    }: CreateReportParams) => {
      if (staged.length === 0 || isPending) return;
      setIsPending(true);
      try {
        // 보관함 저장(순서 보존). 결과별 편집 제목을 보관함 항목 제목으로도 전달.
        const blocks: ReportBlock[] = await Promise.all(
          staged.map(async (runId) => {
            const msg = messageOf(runId);
            const card = cardStateOf(runId);
            const saved = await chatApi.saveResult(projectId, {
              run_id: runId,
              thread_id: threadOf(runId),
              title: card.title.trim() || undefined,
            });
            return {
              uid: createClientId(),
              libId: saved.result_id,
              title: card.title.trim() || null,
              interp: card.note.trim(),
              opts: { q: true, detail: msg ? hasDetail(msg) : false, plan: false },
              span: 12,
              height: null,
              newRow: true,
            };
          }),
        );

        const doc = await reportDocApi.create(projectId, {
          title: reportTitle.trim() || undefined,
          blocks,
        });

        // 보고서/보관함 목록 stale 처리 후 에디터로 이동.
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
