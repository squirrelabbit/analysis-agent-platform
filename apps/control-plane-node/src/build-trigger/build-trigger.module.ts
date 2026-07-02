import { Module } from '@nestjs/common';
import { VersionsRepository } from '../versions/versions.repository';
import { BuildTriggerController } from './build-trigger.controller';
import { BuildTriggerRepository } from './build-trigger.repository';
import { BuildTriggerService } from './build-trigger.service';
import { TemporalStarterService } from './temporal.service';

@Module({
  controllers: [BuildTriggerController],
  providers: [BuildTriggerService, BuildTriggerRepository, TemporalStarterService, VersionsRepository],
})
export class BuildTriggerModule {}
