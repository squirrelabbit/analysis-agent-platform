import { Controller, Get, Param } from '@nestjs/common';
import { DatasetBuildJobDto } from './build-job.dto';
import { BuildJobsService } from './build-jobs.service';

@Controller('projects/:project_id/dataset_build_jobs')
export class BuildJobsController {
  constructor(private readonly service: BuildJobsService) {}

  /** GET /projects/{pid}/dataset_build_jobs/{job_id} — Go handleGetDatasetBuildJob 계약 동일. */
  @Get(':job_id')
  async get(
    @Param('project_id') projectId: string,
    @Param('job_id') jobId: string,
  ): Promise<DatasetBuildJobDto> {
    return this.service.get(projectId, jobId);
  }
}
