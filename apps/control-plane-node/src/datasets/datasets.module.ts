import { Module } from '@nestjs/common';
import { DatasetsController } from './datasets.controller';
import { DatasetsRepository } from './datasets.repository';
import { DatasetsService } from './datasets.service';

@Module({
  controllers: [DatasetsController],
  providers: [DatasetsService, DatasetsRepository],
})
export class DatasetsModule {}
