import type { DatasetFormValues, DatasetMeta } from "../schemas/dataset";
import type { CreateDatasetRequest, DatasetResponse } from "./dto";
import type { Dataset } from "./model";


export const mapDataset = (dto: DatasetResponse): Dataset => {
  const dg = dto.metadata?.doc_genuineness;
  return {
    id: dto.dataset_id,
    projectId: dto.project_id,
    name: dto.name,
    description: dto.description,
    dataType: dto.data_type,
    activeDatasetVersionId: dto.active_dataset_version_id,
    activeVersionUpdatedAt: dto.active_version_updated_at,
    createdAt: dto.created_at,
    docGenuineness: dg
      ? {
          subjectType: dg.subject_type ?? "",
          subjectName: dg.subject_name ?? "",
          subjectAliases: dg.subject_aliases ?? [],
          recruitmentKeywords: dg.recruitment_keywords ?? [],
        }
      : undefined,
  };
};

export  const mapMetadataRequest = (
  metadata: DatasetMeta,
) => ({
  subject_type: metadata.subjectType,
  subject_name: metadata.subjectName,
  subject_aliases: metadata.subjectAliases,
  recruitment_keywords:
    metadata.recruitmentKeywords,
});

export const mapDatasetFormToRequest = (
  form: DatasetFormValues,
): CreateDatasetRequest => ({
  name: form.name,
  description: form.description,
  data_type: form.dataType, 
  metadata: form.metadata?.docGenuineness
    ? {
        doc_genuineness:
          mapMetadataRequest(
            form.metadata.docGenuineness,
          ),
      }
    : undefined,
});
