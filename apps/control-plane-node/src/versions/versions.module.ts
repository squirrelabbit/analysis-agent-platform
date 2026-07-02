import { Module } from '@nestjs/common';
import { KeywordDictionaryModule } from '../keyword-dictionary/keyword-dictionary.module';
import { PythonWorkerClient } from '../worker/worker-client';
import { ArtifactViewsController } from './artifact-views.controller';
import { ArtifactViewsRepository } from './artifact-views.repository';
import { ArtifactViewsService } from './artifact-views.service';
import { DocGenuinenessCompareController } from './doc-genuineness-compare.controller';
import { VersionsController } from './versions.controller';
import { VersionsRepository } from './versions.repository';
import { VersionsService } from './versions.service';

@Module({
  imports: [KeywordDictionaryModule],
  controllers: [VersionsController, ArtifactViewsController, DocGenuinenessCompareController],
  providers: [
    VersionsService,
    VersionsRepository,
    ArtifactViewsService,
    ArtifactViewsRepository,
    PythonWorkerClient,
  ],
})
export class VersionsModule {}
