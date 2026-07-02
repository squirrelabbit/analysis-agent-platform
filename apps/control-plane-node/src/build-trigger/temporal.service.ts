import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Client, Connection } from '@temporalio/client';

/** Go workflows.StartDatasetBuildInput 대응 payload (wire JSON은 snake_case). */
export interface StartDatasetBuildInput {
  jobId: string;
  projectId: string;
  datasetId: string;
  datasetVersionId: string;
  buildType: string;
  requestId: string;
}

/**
 * Go workflows.TemporalStarter 대응 — dataset.build.v1 워크플로 start.
 * 워크플로/액티비티는 Go temporal-worker가 실행한다 (strangler: client부터 Node).
 * 연결은 첫 start에서 lazy로 맺고 재사용한다.
 */
@Injectable()
export class TemporalStarterService implements OnModuleDestroy {
  private readonly address = process.env.TEMPORAL_ADDRESS ?? '127.0.0.1:17233';
  private readonly namespace = process.env.TEMPORAL_NAMESPACE ?? 'default';
  private readonly taskQueue =
    process.env.TEMPORAL_BUILD_TASK_QUEUE ??
    process.env.TEMPORAL_TASK_QUEUE ??
    'analysis-support-build';
  private connection: Connection | null = null;

  /** Go buildDatasetBuildWorkflowID + StartDatasetBuildWorkflow — workflow_id 반환. */
  async startDatasetBuild(input: StartDatasetBuildInput): Promise<string> {
    if (this.connection === null) {
      this.connection = await Connection.connect({ address: this.address });
    }
    const client = new Client({ connection: this.connection, namespace: this.namespace });
    const workflowId = `dataset-build-${input.jobId}`;
    await client.workflow.start('dataset.build.v1', {
      taskQueue: this.taskQueue,
      workflowId,
      args: [
        {
          job_id: input.jobId,
          project_id: input.projectId,
          dataset_id: input.datasetId,
          dataset_version_id: input.datasetVersionId,
          build_type: input.buildType,
          request_id: input.requestId,
          requested_at: new Date().toISOString(),
        },
      ],
    });
    return workflowId;
  }

  async onModuleDestroy(): Promise<void> {
    await this.connection?.close().catch(() => undefined);
  }
}
