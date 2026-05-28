import type { DataType } from "@/shared/types/common";

export interface Dataset {
  id: string,
  projectId: string,
  name: string,
  description: string,
  dataType: DataType,
  activeDatasetVersionId: string,
  activeVersionUpdatedAt: string,
  createdAt: string
}

