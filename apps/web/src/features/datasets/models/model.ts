import type { DataType } from "@/shared/types/common";

export interface DatasetDocGenuineness {
  subjectType: string;
  subjectName: string;
  subjectAliases: string[];
  recruitmentKeywords: string[];
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

