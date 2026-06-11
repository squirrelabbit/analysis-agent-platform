export type ChatRole = "user" | "assistant";

export interface AnalysisMessageViewDto {
  message_id: string;
  role: ChatRole;
  content: string;
  run_id?: string | null;
  created_at: string;
}

export interface AnalysisRunViewDto {
  run_id: string;
  thread_id: string;
  status: "running" | "completed" | "failed";
  error_message?: string | null;
  created_at: string;
  completed_at?: string | null;
}

export interface AnalysisThreadMessageResponseDto {
  project_id: string;
  dataset_id: string;
  thread_id: string;
  dataset_version_id: string;
  mode: string;
  user_message: AnalysisMessageViewDto;
  assistant_message?: AnalysisMessageViewDto;
  run?: AnalysisRunViewDto;
  result?: {
    composer?: {
      assistant_content: string;
      display?: ComposerDisplayDto;
    };
    plan?: AnalysisPlanDto;
    taxonomy_check?: TaxonomyCheckDto;
  };
}

export interface AnalysisPlanStepDisplayDto {
  label?: string;
  expression?: string;
}

export interface AnalysisPlanStepDto {
  id: string;
  skill: string;
  params: Record<string, unknown>;
  display?: AnalysisPlanStepDisplayDto;
}

export interface AnalysisPlanDto {
  plan_version: string;
  steps: AnalysisPlanStepDto[];
}

// silverone Phase 3-B: clause_label artifact taxonomy 정합성 4-branch status.
export interface TaxonomyCheckDto {
  status?: "ok" | "legacy_missing" | "hash_mismatch" | "id_mismatch";
  [key: string]: unknown;
}

// silverone 2026-06-01 chart-ready metadata v1.
// 신규 turn 응답에는 채워지지만 과거 thread detail 메시지에는 누락될 수
// 있으므로 항상 optional로 다룬다.
export interface ChartSpecDto {
  kind: "bar" | "line" | "diverging_bar" | "metric" | "evidence";
  // bar/line/diverging_bar — 축.
  x?: string;
  y?: string | string[];
  series?: string | null;
  // silverone 2026-06-09 — diverging_bar 계약: 단위(건/%p/%) + 정렬(abs_desc).
  unit?: string | null;
  sort?: string | null;
  // silverone 2026-06-09 — distribution(비중) 막대: y=비중(%), count_col=건수
  // 라벨 보조("X.X% (N건)").
  count_col?: string;
  // line — 기준일 기준선 (YYYY-MM-DD). 있을 때만.
  event_date?: string | null;
  // 기준선 라벨(도메인별). 없으면 프론트가 "기준일"로 fallback.
  event_label?: string | null;
  // metric (total 비교) — 컬럼명 참조.
  a_value?: string;
  b_value?: string;
  delta_value?: string;
  delta_rate?: string;
  // evidence (원문 샘플) — 컬럼명 참조.
  text?: string;
  sentiment?: string;
  chips?: string[];
  id?: string;
}

export interface ComposerDisplayDto {
  type: "table" | "chart" | "json";
  title?: string | null;
  columns?: string[];
  rows?: Record<string, unknown>[];
  total_rows?: number;
  returned_rows?: number;
  max_rows?: number;
  truncated?: boolean;
  warnings?: string[];
  recommended_view?: "table" | "bar" | "diverging_bar" | "line" | "metric" | "evidence" | null;
  chart_spec?: ChartSpecDto | null;
  // silverone 2026-06-09 — 기간/그룹 비교 결과의 컬럼별 표시 포맷/라벨 contract.
  // 백엔드가 단위 의미를 선언하고 프론트가 %·%p·정수로 렌더한다(compare 결과에만 존재).
  // format enum: percent(0~1→%) | point(0~1→%p) | int(정수) | number.
  column_formats?: Record<string, string> | null;
  column_labels?: Record<string, string> | null;
}

export interface AnalyzeUserQuestionRequest {
  user_question: string;
}

export interface AnalysisThreadMessageRequest {
  content: string;
}

export interface AnalysisThreadDto {
  thread_id: string;
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  title?: string;
  created_at: string;
  updated_at: string;
  message_count?: number;
  last_message?: string;
}

export interface AnalysisThreadListResponseDto {
  items: AnalysisThreadDto[];
}

// AnalysisMessage(상세)는 frontend-safe view가 아니라 raw schema이므로 추가 필드를
// 가질 수 있다. 우리가 화면에 쓰는 키만 좁혀 둔다. assistant message에는
// run.result_json.composer.display projection이 동봉된다 (silverone 2026-06-01).
export interface AnalysisMessageDto {
  message_id: string;
  role: ChatRole;
  content: string;
  run_id?: string | null;
  created_at: string;
  display?: ComposerDisplayDto;
  plan?: AnalysisPlanDto;
}

export interface AnalysisThreadDetailDto extends AnalysisThreadDto {
  messages: AnalysisMessageDto[];
}

// silverone 2026-06-10 — 보고서 보관함 저장. 채팅 쪽에서는 저장 요청 + 최소
// 응답(식별/제목)만 사용한다. 보고서 탭의 전체 보관함 조회 계약(display/plan
// 포함)은 보고서 탭 API 연동 시 별도로 다룬다.
export interface ReportSavedResultCreateRequestDto {
  run_id: string;
  thread_id?: string;
  title?: string;
}

export interface ReportSavedResultDto {
  result_id: string;
  title: string;
  run_id?: string;
  thread_id?: string;
  created_at: string;
}
