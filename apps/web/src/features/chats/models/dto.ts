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
  };
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
}

export interface AnalyzeUserQuestionRequest {
  user_question: string;
}

export interface AnalysisThreadMessageRequest {
  content: string;
}
