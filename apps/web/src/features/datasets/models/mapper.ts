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
          taxonomyId: dto.metadata?.taxonomy_id,
        }
      : undefined,
  };
};

export const mapMetadataRequest = (metadata: DatasetMeta) => ({
  subject_type: metadata.subjectType,
  subject_name: metadata.subjectName,
  subject_aliases: metadata.subjectAliases,
  recruitment_keywords: metadata.recruitmentKeywords,
});

// 데이터셋 metadata 요청 객체. doc_genuineness(subject 변수) + taxonomy_id(per-dataset)
// 를 같은 metadata 최상위로 묶는다. taxonomy_id가 빈 값이면 omit(백엔드 default).
export const mapDatasetMetadataRequest = (metadata: DatasetMeta) => {
  const taxonomyId = metadata.taxonomyId?.trim();
  return {
    doc_genuineness: mapMetadataRequest(metadata),
    ...(taxonomyId ? { taxonomy_id: taxonomyId } : {}),
  };
};

export const mapDatasetFormToRequest = (
  form: DatasetFormValues,
): CreateDatasetRequest => ({
  name: form.name,
  description: form.description,
  data_type: form.dataType,
  metadata: form.metadata?.docGenuineness
    ? mapDatasetMetadataRequest(form.metadata.docGenuineness)
    : undefined,
});
