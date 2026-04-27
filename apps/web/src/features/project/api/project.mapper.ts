import type { Project } from "../types/project";
import type { ProjectResponse } from "../types/project.dto";

export const mapProject = (dto: ProjectResponse): Project => ({
  id: dto.project_id,
  name: dto.name,
  description: dto.description,
  datasetCount: dto.dataset_version_count,
  promptCount: dto.prompt_count,
  scenarioCount: dto.scenario_count,
})