import type {
  ReportDto,
  ReportListResponseDto,
  ReportSavedResultDto,
  ReportSavedResultListResponseDto,
  ReportSummaryDto,
} from "./dto";
import type { Report, ReportSavedResult, ReportSummary } from "./model";

// 식별/메타는 snake→camel. display/plan은 분석 스냅샷이라 변환 없이 그대로 보존한다.
export const mapReportSavedResult = (
  dto: ReportSavedResultDto,
): ReportSavedResult => ({
  resultId: dto.result_id,
  projectId: dto.project_id,
  datasetId: dto.dataset_id,
  datasetVersionId: dto.dataset_version_id,
  threadId: dto.thread_id,
  runId: dto.run_id,
  sourceMessageId: dto.source_message_id,
  title: dto.title,
  question: dto.question,
  assistantContent: dto.assistant_content,
  display: dto.display,
  plan: dto.plan,
  createdAt: dto.created_at,
});

export const mapReportSavedResultList = (
  dto: ReportSavedResultListResponseDto,
): ReportSavedResult[] => dto.items?.map(mapReportSavedResult) ?? [];

// ── 보고서 문서 매퍼 ──
export const mapReportSummary = (dto: ReportSummaryDto): ReportSummary => ({
  reportId: dto.report_id,
  projectId: dto.project_id,
  title: dto.title,
  blockCount: dto.block_count,
  createdAt: dto.created_at,
  updatedAt: dto.updated_at,
});

export const mapReportList = (dto: ReportListResponseDto): ReportSummary[] =>
  dto.items?.map(mapReportSummary) ?? [];

export const mapReport = (dto: ReportDto): Report => ({
  reportId: dto.report_id,
  projectId: dto.project_id,
  title: dto.title,
  blocks: dto.blocks ?? [],
  createdAt: dto.created_at,
  updatedAt: dto.updated_at,
});
