import { Controller, Get } from '@nestjs/common';
import { ProjectListResponse } from './project.dto';
import { ProjectsService } from './projects.service';

@Controller('projects')
export class ProjectsController {
  constructor(private readonly service: ProjectsService) {}

  /** GET /projects — Go handleListProjects 계약 동일. */
  @Get()
  async list(): Promise<ProjectListResponse> {
    return this.service.list();
  }
}
