import { Controller, Get, Param } from '@nestjs/common';
import { ProjectDto, ProjectListResponse } from './project.dto';
import { ProjectsService } from './projects.service';

@Controller('projects')
export class ProjectsController {
  constructor(private readonly service: ProjectsService) {}

  /** GET /projects — Go handleListProjects 계약 동일. */
  @Get()
  async list(): Promise<ProjectListResponse> {
    return this.service.list();
  }

  /** GET /projects/{project_id} — Go handleGetProject 계약 동일 (counts 포함). */
  @Get(':project_id')
  async get(@Param('project_id') projectId: string): Promise<ProjectDto> {
    return this.service.get(projectId);
  }
}
