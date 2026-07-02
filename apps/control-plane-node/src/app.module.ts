import { Module } from '@nestjs/common';
import { BuildJobsModule } from './build-jobs/build-jobs.module';
import { DatasetsModule } from './datasets/datasets.module';
import { DbModule } from './db/db.module';
import { HealthController } from './health/health.controller';
import { KeywordDictionaryModule } from './keyword-dictionary/keyword-dictionary.module';
import { ProjectsModule } from './projects/projects.module';
import { ProxyModule } from './proxy/proxy.module';
import { ReportsModule } from './reports/reports.module';
import { VersionsModule } from './versions/versions.module';

@Module({
  imports: [
    DbModule,
    ProjectsModule,
    DatasetsModule,
    BuildJobsModule,
    VersionsModule,
    KeywordDictionaryModule,
    ProxyModule,
    ReportsModule,
  ],
  controllers: [HealthController],
})
export class AppModule {}
