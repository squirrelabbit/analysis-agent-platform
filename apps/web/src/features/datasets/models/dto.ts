import type { DataType } from "@/shared/types/common";

export interface DocGenuinenessMeta {
  subject_type?: string;
  subject_name?: string;
  subject_aliases?: string[];
  recruitment_keywords?: string[];
}

export interface DatasetResponse {
  dataset_id: string;
  project_id: string;
  name: string;
  description: string;
  data_type: DataType;
  active_dataset_version_id: string;
  active_version_updated_at: string;
  created_at: string;
  metadata?: {
    doc_genuineness?: DocGenuinenessMeta;
    taxonomy_id?: string;
  };
}

export interface DatasetListResponse {
  items: DatasetResponse[];
}

export interface MetadataRequest {
  metadata?: {
    doc_genuineness: {
      subject_type: string;
      subject_name: string;
      subject_aliases: string[];
      recruitment_keywords: string[];
    };
    taxonomy_id?: string;
  };
}

export interface InfoRequest {
  name: string;
  description: string;
  data_type: DataType;
}

export type CreateDatasetRequest =
  MetadataRequest &
  InfoRequest;