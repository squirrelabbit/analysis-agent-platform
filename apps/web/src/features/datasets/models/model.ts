import type { DataType } from "@/shared/types/common";

export interface DatasetDocGenuineness {
  subjectType: string;
  subjectName: string;
  subjectAliases: string[];
  recruitmentKeywords: string[];
  // per-dataset aspect taxonomy (metadata.taxonomy_id). 폼 바인딩용 — doc_genuineness
  // 와 별개 metadata 키지만 메타 폼 한 곳에서 다루므로 같은 모델에 둔다.
  taxonomyId?: string;
}

export interface Dataset {
  id: string,
  projectId: string,
  name: string,
  description: string,
  dataType: DataType,
  activeDatasetVersionId: string,
  activeVersionUpdatedAt: string,
  createdAt: string,
  docGenuineness?: DatasetDocGenuineness,
}

