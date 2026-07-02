export interface ProjectResponse {
  project_id: string,
  name: string,
  description: string,
  dataset_count?: number,
  dataset_version_count: number,
  scenario_count: number,
  prompt_count: number,
  analysis_thread_count: number,
  created_at: string,
  // 상세 조회(GET /projects/{id})에서만 내려온다. 목록 항목에는 없다.
  metadata?: ProjectMetadata,
}

export interface ProjectListResponse {
  items: ProjectResponse[]
}

// 축제 메타(#31, 2026-07-02 재설계) — 프로젝트 레벨. 연도별 대상기간·축제기간 + 역할(기준/비교).
export interface FestivalPeriodInput {
  year: number,
  role: "base" | "compare",  // 기준 / 비교 연도
  target_start: string,      // 대상기간 시작 YYYY-MM-DD
  target_end: string,        // 대상기간 종료 YYYY-MM-DD
  festival_start: string,    // 축제기간 시작 YYYY-MM-DD
  festival_end: string,      // 축제기간 종료 YYYY-MM-DD
}

export interface FestivalMetadataInput {
  name: string,
  periods: FestivalPeriodInput[],
}

export interface ProjectMetadata {
  festival?: FestivalMetadataInput,
  [key: string]: unknown,
}

export interface CreateProjectRequest {
  name: string,
  description: string,
  metadata?: ProjectMetadata,
}

// PATCH /projects/{id} — non-nil 필드만 반영. metadata는 백엔드에서 key 단위 merge.
export interface UpdateProjectRequest {
  name?: string,
  description?: string,
  metadata?: ProjectMetadata,
}
