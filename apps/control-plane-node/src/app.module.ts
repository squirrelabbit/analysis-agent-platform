import { Module } from '@nestjs/common';
import { DbModule } from './db/db.module';
import { HealthController } from './health/health.controller';
import { ProjectsModule } from './projects/projects.module';

@Module({
  imports: [DbModule, ProjectsModule],
  controllers: [HealthController],
})
export class AppModule {}
