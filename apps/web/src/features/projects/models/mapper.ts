import type { Project } from "./model";
import type { ProjectResponse } from "./dto";

export const mapProject = (dto: ProjectResponse): Project => ({
  id: dto.project_id,
  name: dto.name,
  description: dto.description,
  datasetCount: dto.dataset_count ?? dto.dataset_version_count,
  promptCount: dto.prompt_count,
  chatCount: dto.analysis_thread_count,
  createdAt: dto.created_at
})
