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
}

export interface ProjectListResponse {
  items: ProjectResponse[]
}

// 축제 메타(#31) — 프로젝트 레벨. 연도별 축제기간(during) + ±N일(선택, 비우면 개방형).
export interface FestivalPeriodInput {
  year: number,
  festival_start: string,   // YYYY-MM-DD
  festival_end: string,     // YYYY-MM-DD
  before_days?: number,
  after_days?: number,
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
