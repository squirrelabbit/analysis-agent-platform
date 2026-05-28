import type { VersionFormValues } from "../../schemas/version.schema";
import { mapClauseLabelVersionBuild, mapCleanVersionBuild, mapGenuinenessVersionBuild } from "../build";
import type { CreateVersionRequest, VersionDetailResponse, VersionResponse } from "./dto";
import type { Version, VersionDetail } from "./model";

export const mapVersion = (dto: VersionResponse): Version => ({
  id: dto.dataset_version_id,
  createdAt: dto.created_at,
  isActive: dto.is_active,
  rowCount: dto.row_count,
  columnCount: dto.column_count,
  columns: dto.columns,
  byteSize: dto.byte_size,
  cleanStatus: dto.clean_status,
  docGenuinenessStatus: dto.doc_genuineness_status,
  clauseLabelStatus: dto.clause_label_status,
  originalFilename: dto.original_filename,
});

export const mapVersionDetail = (dto: VersionDetailResponse): VersionDetail => ({
  id: dto.dataset_version_id,
  createdAt: dto.created_at,
  isActive: dto.is_active,
  rowCount: dto.row_count ?? 0,
  columnCount: dto.column_count ?? 0,
  columns: dto.columns ?? [],
  byteSize: dto.byte_size ?? 0,
  clean: mapCleanVersionBuild(dto.clean),
  docGenuineness: mapGenuinenessVersionBuild(dto.doc_genuineness),
  clauseLabel: mapClauseLabelVersionBuild(dto.clause_label),
});

export const mapVersionFormToRequest = (
  form: VersionFormValues,
): CreateVersionRequest => ({
  file: form.file,
  data_type: form.dataType,
  activate_on_create: form.activateOnCreate,
});
