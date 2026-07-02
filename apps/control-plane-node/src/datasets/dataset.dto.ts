/**
 * GET /projects/{pid}/datasets 응답 계약 — Go domain.Dataset과 필드·JSON 키 동일.
 * omitempty 규칙: description은 NULL일 때만 생략(Go *string omitempty — DB에 ''가
 * 있으면 ""로 노출), active_dataset_version_id / active_version_updated_at는 NULL이면
 * 생략, metadata는 NULL/빈 객체면 생략(Go가 NULL을 {}로 unmarshal 후 omitempty).
 */
export interface DatasetDto {
  dataset_id: string;
  project_id: string;
  name: string;
  description?: string;
  data_type: string;
  active_dataset_version_id?: string;
  active_version_updated_at?: string;
  metadata?: Record<string, unknown>;
  created_at: string;
}

export interface DatasetListResponse {
  items: DatasetDto[];
}
