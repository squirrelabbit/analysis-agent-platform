import { Module } from '@nestjs/common';
import { PythonWorkerClient } from '../worker/worker-client';
import { VersionsController } from './versions.controller';
import { VersionsRepository } from './versions.repository';
import { VersionsService } from './versions.service';

@Module({
  controllers: [VersionsController],
  providers: [VersionsService, VersionsRepository, PythonWorkerClient],
})
export class VersionsModule {}
