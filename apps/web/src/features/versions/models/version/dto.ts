import type { DataType } from "@/shared/types/common";
import type {
  ClauseVersionBuildDto,
  CleanVersionBuildDto,
  GenuinenessVersionBuildDto,
} from "../build/dto";

export interface VersionResponse {
  dataset_version_id: string;
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

export interface VersionListResponse {
  items: VersionResponse[];
}

export interface CreateVersionRequest {
  file: File;
  data_type: DataType;
  activate_on_create: boolean;
}

export interface VersionDetailResponse {
  dataset_version_id: string;
  created_at: string;
  is_active: boolean;
  row_count: number;
  column_count?: number;
  columns?: string[];
  byte_size: number;
  clean: CleanVersionBuildDto;
  doc_genuineness: GenuinenessVersionBuildDto;
  clause_label: ClauseVersionBuildDto;
}
