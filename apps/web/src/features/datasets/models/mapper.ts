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

export const mapMetadataRequest = (metadata: DatasetMeta) => {
  // 행사별 추가 슬롯(doc_genuineness 전용). 빈 값이면 omit → 프롬프트 슬롯 섹션 생략.
  const instr = metadata.genuinenessExtraInstructions?.trim();
  const examples = metadata.genuinenessExtraExamples?.trim();
  return {
    subject_type: metadata.subjectType,
    subject_name: metadata.subjectName,
    subject_aliases: metadata.subjectAliases,
    recruitment_keywords: metadata.recruitmentKeywords,
    ...(instr ? { extra_instructions: instr } : {}),
    ...(examples ? { extra_examples: examples } : {}),
  };
};

// 데이터셋 metadata 요청 객체. doc_genuineness(subject 변수 + 진성용 슬롯) +
// clause_label(절 라벨링용 슬롯) + taxonomy_id(per-dataset)를 같은 metadata 최상위로
// 묶는다. 빈 값인 키는 omit. clause_label.extra_*는 doc_genuineness.extra_*와 분리한다
// (출력 스키마가 달라 공용 금지).
export const mapDatasetMetadataRequest = (metadata: DatasetMeta) => {
  const taxonomyId = metadata.taxonomyId?.trim();
  const clauseInstr = metadata.clauseExtraInstructions?.trim();
  const clauseExamples = metadata.clauseExtraExamples?.trim();
  const clauseLabel = {
    ...(clauseInstr ? { extra_instructions: clauseInstr } : {}),
    ...(clauseExamples ? { extra_examples: clauseExamples } : {}),
  };
  return {
    doc_genuineness: mapMetadataRequest(metadata),
    ...(taxonomyId ? { taxonomy_id: taxonomyId } : {}),
    ...(Object.keys(clauseLabel).length ? { clause_label: clauseLabel } : {}),
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
