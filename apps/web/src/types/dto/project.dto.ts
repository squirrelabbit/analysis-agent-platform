// POST /projects — body
export interface CreateProjectPayload {
  name: string
  description: string
}

// GET /projects/:id — response data
export interface ProjectResponse {
  project_id: string
  name: string
  description: string
  dataset_version_count: number
  scenario_count: number
  prompt_count: number
  created_at: string,
}

// GET /projects — response data
export interface ProjectListResponse {
  items: ProjectResponse[]
}