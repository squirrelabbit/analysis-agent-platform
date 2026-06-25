import type { DataType } from "@/shared/types/common";

export interface DocGenuinenessMeta {
  subject_type?: string;
  subject_name?: string;
  subject_aliases?: string[];
  recruitment_keywords?: string[];
  // 행사별 추가 슬롯 (doc_genuineness 전용).
  extra_instructions?: string;
  extra_examples?: string;
}

// 행사별 추가 슬롯 (clause_label 전용). doc_genuineness.extra_*와 분리.
export interface ClauseLabelMeta {
  extra_instructions?: string;
  extra_examples?: string;
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
    clause_label?: ClauseLabelMeta;
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
      extra_instructions?: string;
      extra_examples?: string;
    };
    clause_label?: ClauseLabelMeta;
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