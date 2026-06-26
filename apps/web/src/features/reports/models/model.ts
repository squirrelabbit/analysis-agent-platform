// 보고서 문서(목록용 경량 projection). blocks 본문 대신 개수만.
export interface ReportSummary {
  reportId: string;
  projectId: string;
  title: string;
  blockCount: number;
  createdAt: string;
  updatedAt: string;
}

// 보고서 문서 단건(blocks 포함). blocks는 에디터가 소유하는 opaque snapshot.
export interface Report {
  reportId: string;
  projectId: string;
  title: string;
  datasetVersionId?: string;
  blocks: unknown[];
  createdAt: string;
  updatedAt: string;
}
