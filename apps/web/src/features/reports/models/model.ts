import type { AnalysisPlanDto, ComposerDisplayDto } from "@/features/chats/models";

// 보고서 보관함 항목(도메인). 식별/메타는 camelCase로 정리한다.
// display/plan은 저장 시점 분석 스냅샷으로, 채팅 composer.display / plan과 동일 shape를
// 그대로 보존한다(별도 도메인 변환 없이 opaque snapshot). 렌더 시 채팅과 동일하게 투영한다.
export interface ReportSavedResult {
  resultId: string;
  projectId: string;
  datasetId: string;
  datasetVersionId: string;
  threadId: string;
  runId: string;
  sourceMessageId: string;
  title: string;
  question: string;
  assistantContent: string;
  display?: ComposerDisplayDto;
  plan?: AnalysisPlanDto;
  createdAt: string;
}

// 보고서 문서(목록용 경량 projection). blocks 본문 대신 개수만.
export interface ReportSummary {
  reportId: string;
  projectId: string;
  title: string;
  blockCount: number;
  createdAt: string;
  updatedAt: string;
}

// 보고서 문서 단건(blocks 포함). blocks는 에디터가 소유하는 opaque snapshot.
export interface Report {
  reportId: string;
  projectId: string;
  title: string;
  blocks: unknown[];
  createdAt: string;
  updatedAt: string;
}
