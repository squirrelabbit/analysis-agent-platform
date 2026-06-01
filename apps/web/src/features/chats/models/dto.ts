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

export interface AnalysisPlanStepDto {
  id: string;
  skill: string;
  params: Record<string, unknown>;
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
  kind: "bar" | "line";
  x: string;
  y: string | string[];
  series: string | null;
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
  recommended_view?: "table" | "bar" | "line" | null;
  chart_spec?: ChartSpecDto | null;
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
