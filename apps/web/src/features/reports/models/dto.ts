// ── 보고서 문서(Report) — 채팅 분석 결과 item / 기본 템플릿 블록 문서 CRUD ──
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
  // 기본 템플릿으로 만든 보고서가 묶인 dataset_version(일반 보고서면 빈 문자열/생략).
  dataset_version_id?: string;
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

// POST /projects/{project_id}/reports/from_template — dataset_id의 active version으로
// 기본 템플릿 보고서를 생성한다(active version이 clean ready여야 한다).
export interface ReportFromTemplateRequestDto {
  template_id: string;
  dataset_id: string;
}

export interface ReportMissingSectionDto {
  section_id: string;
  reason: string;
}

export interface ReportFromTemplateResponseDto {
  report: ReportDto;
  included_sections: string[];
  missing_sections: ReportMissingSectionDto[];
}

// POST /projects/{project_id}/reports/{report_id}/item — 기존 보고서 blocks 뒤에 item 1개 append.
// 보통 run_id만 보내면 type=analysis_result로 추가된다.
export interface ReportItemAppendRequestDto {
  run_id?: string;
  type?: string;
  thread_id?: string;
  title?: string;
  interp?: string;
  options?: Record<string, unknown>;
  layout?: Record<string, unknown>;
}

export interface ReportItemAppendResponseDto {
  report: ReportDto;
  // 추가된 item 블록(opaque — 블록 렌더 계약은 프론트 에디터 소유).
  item: Record<string, unknown>;
}
