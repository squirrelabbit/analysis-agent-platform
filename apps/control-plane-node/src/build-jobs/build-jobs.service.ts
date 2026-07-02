import { Injectable } from '@nestjs/common';
import { notFound } from '../common/errors';
import { goTimestamptz } from '../common/go-time';
import { anyToInt, metadataBool, metadataString } from '../common/metadata';
import { loadBuildProgress } from '../common/progress';
import {
  BuildJobDiagnosticsDto,
  BuildJobProgressDto,
  DatasetBuildJobDto,
} from './build-job.dto';
import { BuildJobRow, BuildJobsRepository } from './build-jobs.repository';

/** Go buildJobMetadataPrefix — build_type → version metadata key prefix. */
const BUILD_TYPE_METADATA_PREFIX: Record<string, string> = {
  clean: 'clean',
  clause_label: 'clause_label',
  doc_genuineness: 'doc_genuineness',
  clause_keywords: 'clause_keywords',
};

@Injectable()
export class BuildJobsService {
  constructor(private readonly repo: BuildJobsRepository) {}

  async get(projectId: string, jobId: string): Promise<DatasetBuildJobDto> {
    const row = await this.repo.get(projectId, jobId);
    if (row === undefined) {
      throw notFound('dataset build job');
    }
    const dto = this.toDto(row);
    // Go GetDatasetBuildJob — version 조회 성공 시에만 metadata 기반 enrich, 실패는 조용히 skip.
    const versionMeta = await this.repo.getVersionMetadata(projectId, row.dataset_version_id);
    if (versionMeta !== undefined && dto.diagnostics) {
      this.enrichDiagnosticsFromVersionMetadata(dto.diagnostics, versionMeta ?? {}, row.build_type);
    }
    return dto;
  }

  private toDto(row: BuildJobRow): DatasetBuildJobDto {
    const dto: DatasetBuildJobDto = {
      job_id: row.job_id,
      project_id: row.project_id,
      dataset_id: row.dataset_id,
      dataset_version_id: row.dataset_version_id,
      build_type: row.build_type,
      status: row.status,
      attempt: row.attempt,
      created_at: goTimestamptz(row.created_at),
    };
    if (row.request != null && Object.keys(row.request).length > 0) {
      dto.request = row.request;
    }
    if (row.triggered_by != null && row.triggered_by !== '') {
      dto.triggered_by = row.triggered_by;
    }
    if (row.workflow_id != null) {
      dto.workflow_id = row.workflow_id;
    }
    if (row.workflow_run_id != null) {
      dto.workflow_run_id = row.workflow_run_id;
    }
    if (row.last_error_type != null) {
      dto.last_error_type = row.last_error_type;
    }
    if (row.started_at != null) {
      dto.started_at = goTimestamptz(row.started_at);
    }
    if (row.completed_at != null) {
      dto.completed_at = goTimestamptz(row.completed_at);
    }
    if (row.error_message != null) {
      dto.error_message = row.error_message;
    }
    dto.diagnostics = this.baseDiagnostics(row);
    return dto;
  }

  /** Go withBuildJobDiagnostics — diagnostics 쪽 문자열은 trim된 사본(cloneStringPointer). */
  private baseDiagnostics(row: BuildJobRow): BuildJobDiagnosticsDto {
    const diagnostics: BuildJobDiagnosticsDto = {
      retry_count: Math.max(row.attempt - 1, 0),
    };
    if (row.last_error_type != null) {
      diagnostics.last_error_type = row.last_error_type.trim();
    }
    if (row.error_message != null) {
      diagnostics.last_error_message = row.error_message.trim();
    }
    if (row.workflow_id != null) {
      diagnostics.workflow_id = row.workflow_id.trim();
    }
    if (row.workflow_run_id != null) {
      diagnostics.workflow_run_id = row.workflow_run_id.trim();
    }
    return diagnostics;
  }

  /** Go enrichBuildJobDiagnosticsFromVersion — progress 파일 + llm fallback 필드 합성. */
  private enrichDiagnosticsFromVersionMetadata(
    diagnostics: BuildJobDiagnosticsDto,
    metadata: Record<string, unknown>,
    buildType: string,
  ): void {
    const prefix = BUILD_TYPE_METADATA_PREFIX[buildType.trim()];
    if (!prefix) {
      return;
    }
    const progress = this.loadProgress(metadata, prefix);
    if (progress) {
      diagnostics.progress = progress;
    }
    if (!metadataBool(metadata, `${prefix}_llm_fallback`)) {
      return; // Go도 fallback 아닐 땐 warnings 포함 나머지를 붙이지 않는다.
    }
    diagnostics.llm_fallback = true;
    const count = anyToInt(metadata[`${prefix}_llm_fallback_count`]);
    if (count !== undefined && count !== 0) {
      diagnostics.llm_fallback_count = count;
    }
    const reason = metadataString(metadata, `${prefix}_llm_fallback_reason`);
    if (reason) {
      diagnostics.llm_fallback_reason = reason;
    }
    const provider = metadataString(metadata, `${prefix}_llm_provider`);
    if (provider) {
      diagnostics.llm_provider = provider;
    }
    const model = metadataString(metadata, `${prefix}_llm_model`);
    if (model) {
      diagnostics.llm_model = model;
    }
    const warning = metadataString(metadata, `${prefix}_warning`);
    if (warning) {
      diagnostics.warnings = [warning];
    }
  }

  /** RawBuildProgress → diagnostics DTO (elapsed_seconds 포함, Go BuildJobProgress omitempty 규칙). */
  private loadProgress(
    metadata: Record<string, unknown>,
    prefix: string,
  ): BuildJobProgressDto | null {
    const raw = loadBuildProgress(metadata, prefix);
    if (!raw) {
      return null;
    }
    const progress: BuildJobProgressDto = {
      percent: raw.percent,
      processed_rows: raw.processed_rows,
      total_rows: raw.total_rows,
    };
    if (raw.elapsed_seconds !== 0) {
      progress.elapsed_seconds = raw.elapsed_seconds;
    }
    if (raw.eta_seconds !== null) {
      progress.eta_seconds = raw.eta_seconds;
    }
    if (raw.message) {
      progress.message = raw.message;
    }
    if (raw.updated_at !== null) {
      progress.updated_at = raw.updated_at;
    }
    return progress;
  }
}
