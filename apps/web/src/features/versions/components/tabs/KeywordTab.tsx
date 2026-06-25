import { Hash, Layers, Percent, Star } from "lucide-react";
import { StatCard } from "@/components/common/cards/StatCard";
import {
  BuildMetaBar,
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  isBuildRunning,
} from "../BuildStatusMeta";
import type { KeywordBuild } from "../../models/build";
import { useBuildVersion } from "../../hooks/build.query";
import KeywordClauseTable from "../KeywordClauseTable";
import KeywordSentimentRankTable from "../KeywordSentimentRankTable";

export function KeywordTab() {
  // 요약 통계는 기존 clause_keywords 집계 API를 그대로 사용한다.
  // "절에서 추출된 키워드" 표는 추출 API가 아직 없어 KeywordClauseTable에서 하드코딩.
  // (Aspect별 키워드 워드클라우드·긍부정 Top·키워드 상세 표는 제거됨.)
  const { data, isLoading } = useBuildVersion("clause_keywords") as {
    data: KeywordBuild | undefined;
    isLoading: boolean;
  };
  const {
    summary,
    items = [],
    applied,
    status,
    progress,
    durationSeconds,
  } = data || {};

  if (isLoading) return <BuildTabLoading />;

  // 빌드 전(summary 없음)·진행 중이 아니면 다른 탭과 동일한 empty state.
  // (키워드 순위/절 표가 빌드 안 한 버전에서 빈 표로 노출되던 것 방지)
  if (!summary && !isBuildRunning(status)) {
    return <BuildTabEmpty type="clause_keywords" status={status ?? "not_requested"} />;
  }

  // 최다 출현 키워드 — 전역 필드가 없어 현재 페이지 상위로 근사.
  const topTerms = items.slice(0, 2).map((it) => it.keyword);

  return (
    <div className="space-y-5">
      {summary && (
        <>
          {/* 메타 */}
          <BuildMetaBar
            status={status}
            durationSeconds={durationSeconds}
            applied={applied}
          />
          {/* 요약 통계 */}
          <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
            <StatCard
              value={summary.totalKeywordCount.toLocaleString()}
              label="총 키워드 추출"
              icon={Hash}
              tone="neutral"
            />
            <StatCard
              value={summary.uniqueKeywordCount.toLocaleString()}
              label="고유 키워드 수"
              icon={Layers}
              tone="blue"
            />
            <StatCard
              value={topTerms.join(" · ") || "-"}
              label="최다 출현 키워드"
              icon={Star}
              tone="muted"
            />
            <StatCard
              value={summary.clauseCount.toLocaleString()}
              label="분석 절 수"
              icon={Percent}
              tone="ok"
              valueColor="text-emerald-600"
            />
          </div>
        </>
      )}

      {/* 빌드 진행 중이고 이전 결과(summary)가 아직 없을 때 */}
      {!summary && isBuildRunning(status) && (
        <BuildRunningBanner
          status={status}
          progress={progress}
          hasPrevious={false}
        />
      )}

      {/* 키워드별 긍정/부정 순위 — clause_keywords API(sentiment 필터+limit)로 상위 N 조회 */}
      <KeywordSentimentRankTable />

      {/* 절에서 추출된 키워드 (추출 API 준비 전 — 예시 하드코딩) */}
      <KeywordClauseTable />
    </div>
  );
}
