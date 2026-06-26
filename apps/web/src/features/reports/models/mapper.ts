import type {
  ReportDto,
  ReportListResponseDto,
  ReportSummaryDto,
} from "./dto";
import type { Report, ReportSummary } from "./model";

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
  datasetVersionId: dto.dataset_version_id,
  blocks: dto.blocks ?? [],
  createdAt: dto.created_at,
  updatedAt: dto.updated_at,
});
