import { useQuery } from "@tanstack/react-query";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { versionApi } from "@/features/versions/api/version.api";
import { versionKeys } from "@/features/versions/api/version.key";
import type { VersionResponse } from "@/features/versions/models/version";

/*
 * 데이터셋 카드의 "버전 N개 / 활성 버전 파일 / 최근 업로드"는 dataset 응답에 없다.
 * 백엔드 집계 필드가 생기기 전까지 카드별로 버전 목록을 조회해 파생한다(데이터셋 수가 적어 허용).
 * 확인 필요: dataset 응답에 version_count / active_version_filename 집계 추가 시 이 훅 제거 가능.
 */
export interface DatasetVersionStats {
  versionCount: number;
  activeVersionNumber: number; // 활성 버전의 vN (없으면 0)
  activeFileName: string;
  latestUpload: string; // YYYY-MM-DD
}

type RawVersion = VersionResponse & { version_number?: number };

function derive(rawItems: VersionResponse[]): DatasetVersionStats {
  const items = rawItems as RawVersion[];
  // version_number 없는 legacy row를 위해 created_at ASC 순위(1..n)를 폴백으로 계산.
  const createdAsc = [...items].sort(
    (a, b) =>
      a.created_at.localeCompare(b.created_at) ||
      a.dataset_version_id.localeCompare(b.dataset_version_id),
  );
  const rankById = new Map<string, number>();
  createdAsc.forEach((it, idx) => rankById.set(it.dataset_version_id, idx + 1));
  const numberOf = (it: RawVersion) =>
    typeof it.version_number === "number" && it.version_number > 0
      ? it.version_number
      : (rankById.get(it.dataset_version_id) ?? 0);

  const active = items.find((v) => v.is_active);
  const latest = items.reduce<string>(
    (acc, v) => (v.created_at > acc ? v.created_at : acc),
    "",
  );
  return {
    versionCount: items.length,
    activeVersionNumber: active ? numberOf(active) : 0,
    activeFileName: active?.original_filename ?? "—",
    latestUpload: latest ? latest.slice(0, 10) : "—",
  };
}

export const useDatasetVersionStats = (datasetId: string) => {
  const { projectId } = useProjectParams();
  return useQuery({
    queryKey: [...versionKeys.list(projectId, datasetId), "stats"],
    queryFn: () => versionApi.getVersions(projectId, datasetId),
    select: derive,
    enabled: !!datasetId,
  });
};
