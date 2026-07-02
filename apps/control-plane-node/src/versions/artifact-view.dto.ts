/**
 * 화면 polling용 artifact view 계약 — Go domain.DatasetArtifactView /
 * ArtifactProgress / ArtifactPagination과 필드·JSON 키 동일.
 * job_id~error_message는 omitempty가 아니라 항상 null로라도 노출된다.
 */

export interface ArtifactProgressDto {
  percent: number;
  processed_rows?: number; // Go int omitempty — 0이면 생략
  total_rows?: number;
  eta_seconds?: number;
  message?: string;
  updated_at?: string;
}

export interface ArtifactPaginationDto {
  limit: number;
  offset: number;
  total: number;
}

export interface DatasetArtifactViewDto {
  build_type: string;
  status: string;
  job_id: string | null;
  started_at: string | null;
  completed_at: string | null;
  duration_seconds: number | null;
  error_message: string | null;
  progress?: ArtifactProgressDto;
  applied?: Record<string, unknown>;
  summary?: Record<string, unknown>;
  items?: Record<string, unknown>[];
  pagination?: ArtifactPaginationDto;
}
