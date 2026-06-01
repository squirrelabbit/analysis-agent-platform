export interface ProjectResponse {
  project_id: string,
  name: string,
  description: string,
  dataset_version_count: number,
  scenario_count: number,
  prompt_count: number,
  analysis_thread_count: number,
  created_at: string,
}

export interface ProjectListResponse {
  items: ProjectResponse[]
}

export interface CreateProjectRequest {
  name: string,
  description: string
}