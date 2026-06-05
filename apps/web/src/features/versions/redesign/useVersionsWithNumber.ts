import { useQuery } from "@tanstack/react-query";
import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { versionApi } from "../api/version.api";
import { versionKeys } from "../api/version.key";
import type { VersionResponse } from "../models/version";

/*
 * 리디자인 화면은 version_number(백엔드 !101, 생성순 1-based)를 v-tag(v3/v2/v1)로 쓴다.
 * 기존 useVersions는 mapVersion이 version_number를 버리므로, 기존 파일을 건드리지 않고
 * 여기서 raw 응답을 직접 매핑한다. 백엔드 배포 전(필드 부재)에도 created_at 순위로 폴백한다.
 */
export interface NumberedVersion {
  id: string;
  versionNumber: number;
  createdAt: string;
  isActive: boolean;
  rowCount: number;
  byteSize: number;
  originalFilename: string;
  cleanStatus: string;
  docGenuinenessStatus: string;
  clauseLabelStatus: string;
}

type RawVersion = VersionResponse & { version_number?: number };

function toNumbered(items: RawVersion[]): NumberedVersion[] {
  // version_number가 없는 legacy row를 위해 created_at ASC 순위(1..n)를 폴백으로 계산.
  const createdAsc = [...items].sort(
    (a, b) =>
      a.created_at.localeCompare(b.created_at) ||
      a.dataset_version_id.localeCompare(b.dataset_version_id),
  );
  const rankById = new Map<string, number>();
  createdAsc.forEach((it, idx) => rankById.set(it.dataset_version_id, idx + 1));

  return items
    .map((it) => ({
      id: it.dataset_version_id,
      versionNumber:
        typeof it.version_number === "number" && it.version_number > 0
          ? it.version_number
          : (rankById.get(it.dataset_version_id) ?? 0),
      createdAt: it.created_at,
      isActive: it.is_active,
      rowCount: it.row_count,
      byteSize: it.byte_size,
      originalFilename: it.original_filename,
      cleanStatus: it.clean_status,
      docGenuinenessStatus: it.doc_genuineness_status,
      clauseLabelStatus: it.clause_label_status,
    }))
    // 화면은 최신 버전이 위로(내림차순).
    .sort((a, b) => b.versionNumber - a.versionNumber);
}

export const useVersionsWithNumber = () => {
  const { projectId, datasetId } = useDatasetParams();
  return useQuery({
    queryKey: [...versionKeys.list(projectId, datasetId), "numbered"],
    queryFn: () => versionApi.getVersions(projectId, datasetId),
    select: (data) => toNumbered(data as RawVersion[]),
  });
};
