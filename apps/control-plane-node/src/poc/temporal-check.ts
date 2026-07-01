import {
  Client,
  Connection,
  defaultPayloadConverter,
} from '@temporalio/client';

/**
 * Temporal 상호운용 de-risk (strangler 최대 미지수).
 * "Node @temporalio/client ↔ 기존 Go temporal-worker" 가 통하는가를 **아무것도 새로
 * 띄우지 않고** 검증한다:
 *   1) describeTaskQueue(analysis-support-build) → Go worker 폴러가 보이나 (server·worker 도달)
 *   2) Go가 start한 dataset.build.v1 실행 목록 (Node가 Go 워크플로 읽나)
 *   3) 그 실행의 시작 입력 페이로드 디코딩 → Go 인코딩 payload를 Node가 해석하나 (핵심 interop)
 */
const ADDRESS = process.env.TEMPORAL_ADDRESS ?? '127.0.0.1:17233';
const NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? 'default';
const BUILD_TASK_QUEUE = process.env.TEMPORAL_BUILD_TASK_QUEUE ?? 'analysis-support-build';
const WORKFLOW_TYPE = 'dataset.build.v1';

async function main(): Promise<void> {
  const connection = await Connection.connect({ address: ADDRESS });
  const client = new Client({ connection, namespace: NAMESPACE });
  console.log(`connected: ${ADDRESS} ns=${NAMESPACE}`);

  // 1) task queue 폴러 = Go worker 도달 확인
  const tq = await connection.workflowService.describeTaskQueue({
    namespace: NAMESPACE,
    taskQueue: { name: BUILD_TASK_QUEUE },
    taskQueueType: 1, // WORKFLOW
  });
  const pollers = (tq.pollers ?? []).map((p) => p.identity);
  console.log(`[1] taskQueue '${BUILD_TASK_QUEUE}' pollers=${pollers.length} ${JSON.stringify(pollers)}`);

  // 2) Go가 start한 dataset.build.v1 실행 목록
  const rows: { id: string; status?: string }[] = [];
  for await (const wf of client.workflow.list({
    query: `WorkflowType = '${WORKFLOW_TYPE}'`,
    pageSize: 5,
  })) {
    rows.push({ id: wf.workflowId, status: wf.status?.name });
    if (rows.length >= 5) break;
  }
  console.log(`[2] '${WORKFLOW_TYPE}' 실행 ${rows.length}건: ${JSON.stringify(rows)}`);

  // 3) 가장 최근 실행의 시작 입력 페이로드 디코딩 (Go 인코딩 → Node 해석 = interop 증명)
  if (rows.length > 0) {
    const handle = client.workflow.getHandle(rows[0].id);
    const hist = await handle.fetchHistory();
    const startEvt = (hist.events ?? []).find(
      (e) => e.workflowExecutionStartedEventAttributes != null,
    );
    const payloads =
      startEvt?.workflowExecutionStartedEventAttributes?.input?.payloads ?? [];
    const decoded = payloads.map((p) => defaultPayloadConverter.fromPayload(p));
    console.log(`[3] 시작 입력 디코딩(Go→Node): ${JSON.stringify(decoded)}`);
  }

  await connection.close();
  console.log('OK — Temporal 상호운용 de-risk 완료');
}

main().catch((err) => {
  console.error('FAIL', err);
  process.exit(1);
});
