import type { AnalysisPlanDto, ComposerDisplayDto } from "@/features/chats/models";

// GET /projects/{project_id}/saved_results — 보고서 보관함(저장 시점 분석 스냅샷).
// display/plan은 채팅 composer.display / plan과 동일 shape를 재사용한다(중복 정의 X).
// 저장(POST)·삭제(DELETE)는 채팅에서 수행하므로 여기선 조회/목록 계약만 둔다.
export interface ReportSavedResultDto {
  result_id: string;
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  thread_id: string;
  run_id: string;
  source_message_id: string;
  title: string;
  question: string;
  assistant_content: string;
  display?: ComposerDisplayDto;
  plan?: AnalysisPlanDto;
  created_at: string;
}

export interface ReportSavedResultListResponseDto {
  items: ReportSavedResultDto[];
}

// ── 보고서 문서(Report) — saved_results를 조합한 블록 문서 CRUD ──
// blocks는 control-plane이 영속만 하는 opaque JSON 배열(블록 contract는 프론트 에디터 소유).
export interface ReportSummaryDto {
  report_id: string;
  project_id: string;
  title: string;
  block_count: number;
  created_at: string;
  updated_at: string;
}

export interface ReportDto {
  report_id: string;
  project_id: string;
  title: string;
  blocks: unknown[];
  created_at: string;
  updated_at: string;
}

export interface ReportListResponseDto {
  items: ReportSummaryDto[];
}

export interface ReportCreateRequestDto {
  title?: string;
  blocks?: unknown[];
}

export interface ReportUpdateRequestDto {
  title?: string;
  blocks?: unknown[];
}
