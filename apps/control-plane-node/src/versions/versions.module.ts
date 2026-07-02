import { Module } from '@nestjs/common';
import { PythonWorkerClient } from '../worker/worker-client';
import { ArtifactViewsController } from './artifact-views.controller';
import { ArtifactViewsRepository } from './artifact-views.repository';
import { ArtifactViewsService } from './artifact-views.service';
import { VersionsController } from './versions.controller';
import { VersionsRepository } from './versions.repository';
import { VersionsService } from './versions.service';

@Module({
  controllers: [VersionsController, ArtifactViewsController],
  providers: [
    VersionsService,
    VersionsRepository,
    ArtifactViewsService,
    ArtifactViewsRepository,
    PythonWorkerClient,
  ],
})
export class VersionsModule {}
