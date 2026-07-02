import { Module } from '@nestjs/common';
import { BuildJobsController } from './build-jobs.controller';
import { BuildJobsRepository } from './build-jobs.repository';
import { BuildJobsService } from './build-jobs.service';

@Module({
  controllers: [BuildJobsController],
  providers: [BuildJobsService, BuildJobsRepository],
})
export class BuildJobsModule {}
