/**
 * GET /projects 응답 계약 — Go domain.Project와 필드·JSON 키 동일(openapi.yaml 동결).
 * description은 Go의 `*string omitempty`라 null이면 키 자체를 생략한다.
 * scenario_count는 scenarios 테이블 삭제(δ-4)로 항상 0(Go도 기본값 0).
 */
export interface ProjectDto {
  project_id: string;
  name: string;
  description?: string;
  created_at: string;
  dataset_count: number;
  dataset_version_count: number;
  scenario_count: number;
  prompt_count: number;
  analysis_thread_count: number;
  metadata?: Record<string, unknown>;
}

export interface ProjectListResponse {
  items: ProjectDto[];
}
