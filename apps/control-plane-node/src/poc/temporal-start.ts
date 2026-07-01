import { randomUUID } from 'crypto';
import { Client, Connection } from '@temporalio/client';

/**
 * Temporal 쓰기경로 de-risk: **Node @temporalio/client가 워크플로를 start하고 기존 Go
 * temporal-worker가 실행하는가**.
 *
 * 안전장치: job row 없는(=fake job_id) 입력으로 start한다. Go 워크플로의 첫 액티비티
 * mark_running이 GetDatasetBuildJob으로 job을 읽다가 ErrNotFound로 멈추므로 **실제 clean
 * 빌드(execute)까지 가지 않는다 → 데이터 변형 0**. Go worker가 액티비티를 스케줄/실행하는
 * 것만 확인하면 "Node start → Go worker 실행" 상호운용이 증명된다. 확인 후 terminate.
 */
const ADDRESS = process.env.TEMPORAL_ADDRESS ?? '127.0.0.1:17233';
const NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? 'default';
const BUILD_TASK_QUEUE = process.env.TEMPORAL_BUILD_TASK_QUEUE ?? 'analysis-support-build';
const WORKFLOW_TYPE = 'dataset.build.v1';

// gunsanbeer (실 project/dataset/version). job_id만 fake → mark_running에서 안전 정지.
const PROJECT_ID = '5ce18f63-11a3-4638-a6d1-5e101efc70ff';
const DATASET_ID = '5a8d6a5a-83b7-4cd9-8fb1-aef685268afb';
const VERSION_ID = 'f74c6d01-7f60-4f9f-bc11-3c91c4aca097';

async function main(): Promise<void> {
  const connection = await Connection.connect({ address: ADDRESS });
  const client = new Client({ connection, namespace: NAMESPACE });

  const jobId = randomUUID(); // fake — dataset_build_jobs에 없음
  const workflowId = `poc-nodestart-${jobId}`;
  const input = {
    job_id: jobId,
    project_id: PROJECT_ID,
    dataset_id: DATASET_ID,
    dataset_version_id: VERSION_ID,
    build_type: 'clean',
    request_id: `poc-${jobId}`,
    requested_at: new Date('2026-07-01T00:00:00Z').toISOString(),
  };

  const handle = await client.workflow.start(WORKFLOW_TYPE, {
    taskQueue: BUILD_TASK_QUEUE,
    workflowId,
    args: [input],
  });
  console.log(`[start] Node가 워크플로 start: ${workflowId} (fake job=${jobId})`);

  // Go worker가 pickup해 mark_running 액티비티를 스케줄/실행했는지 history로 확인
  let picked = false;
  for (let i = 0; i < 20; i++) {
    const hist = await handle.fetchHistory();
    const events = hist.events ?? [];
    const scheduled = events.find((e) => e.activityTaskScheduledEventAttributes != null);
    const failed = events.find((e) => e.activityTaskFailedEventAttributes != null);
    const wtCompleted = events.find((e) => e.workflowTaskCompletedEventAttributes != null);
    if (scheduled || failed) {
      const actType = scheduled?.activityTaskScheduledEventAttributes?.activityType?.name;
      console.log(`[pickup] Go worker가 실행 — 액티비티 스케줄='${actType}' (WorkflowTaskCompleted=${wtCompleted != null})`);
      picked = true;
      break;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  if (!picked) console.log('[pickup] 20회 폴링 내 액티비티 스케줄 미관측');

  // 정리: fake 워크플로 종료 (mark_running 재시도 방지)
  await handle.terminate('poc de-risk 완료 — 정리');
  console.log('[cleanup] 워크플로 terminate');

  await connection.close();
  console.log(picked ? 'OK — Node start → Go worker 실행 상호운용 확정' : 'INCONCLUSIVE');
  process.exit(picked ? 0 : 2);
}

main().catch((err) => {
  console.error('FAIL', err);
  process.exit(1);
});
