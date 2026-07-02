import { Controller, Get, Param } from '@nestjs/common';
import { ReportsService } from './reports.service';

@Controller('projects/:project_id/reports')
export class ReportsController {
  constructor(private readonly service: ReportsService) {}

  /** GET /projects/{pid}/reports — Go handleListReports 계약 동일 (blocks 대신 block_count). */
  @Get()
  async list(@Param('project_id') projectId: string): Promise<unknown> {
    return this.service.list(projectId);
  }

  /** GET /projects/{pid}/reports/{report_id} — Go handleGetReport 계약 동일. */
  @Get(':report_id')
  async get(
    @Param('project_id') projectId: string,
    @Param('report_id') reportId: string,
  ): Promise<unknown> {
    return this.service.get(projectId, reportId);
  }
}
