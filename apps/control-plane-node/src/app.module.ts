import { Module } from '@nestjs/common';
import { BuildJobsModule } from './build-jobs/build-jobs.module';
import { DatasetsModule } from './datasets/datasets.module';
import { DbModule } from './db/db.module';
import { HealthController } from './health/health.controller';
import { ProjectsModule } from './projects/projects.module';

@Module({
  imports: [DbModule, ProjectsModule, DatasetsModule, BuildJobsModule],
  controllers: [HealthController],
})
export class AppModule {}
