import { Module } from '@nestjs/common';
import { PythonWorkerClient } from '../worker/worker-client';
import { ProxyController } from './proxy.controller';

@Module({
  controllers: [ProxyController],
  providers: [PythonWorkerClient],
})
export class ProxyModule {}
