/**
 * dataset versions 응답 계약 — Go domain.DatasetVersionListItem /
 * DatasetVersionDetail / DatasetVersionStageDetail과 필드·JSON 키 동일.
 */

export interface DatasetVersionListItemDto {
  dataset_version_id: string;
  version_number: number;
  created_at: string;
  is_active: boolean;
  row_count: number;
  column_count: number;
  columns: string[];
  byte_size: number;
  clean_status: string;
  doc_genuineness_status: string;
  clause_label_status: string;
  original_filename: string;
}

export interface DatasetVersionListResponse {
  items: DatasetVersionListItemDto[];
}

export interface DatasetVersionStageDetailDto {
  status: string;
  completed_at?: string;
  summary?: unknown;
}

export interface DatasetVersionDetailDto {
  dataset_version_id: string;
  version_number: number;
  created_at: string;
  ready_at?: string;
  is_active: boolean;
  row_count: number;
  column_count: number;
  columns: string[];
  byte_size: number;
  clean: DatasetVersionStageDetailDto;
  doc_genuineness: DatasetVersionStageDetailDto;
  clause_label: DatasetVersionStageDetailDto;
}
