import { useState } from "react";
import { Loader2, ThumbsDown, ThumbsUp } from "lucide-react";
import { useBuildVersion } from "../hooks/build.query";
import type { KeywordBuild } from "../models/build";

// 키워드 분석 탭의 "키워드 추출 결과" 카드 위에 놓는 긍/부정 키워드 순위 표.
// 왼쪽=긍정, 오른쪽=부정. 컬럼: 순위 / 키워드 / 빈도. 빈도 내림차순 상위 N을 보여주되
// 10행을 넘으면 표 본문만 스크롤한다(헤더는 sticky 고정). (silverone 2026-06-23)
//
// silverone 2026-06-24 — MOCK 제거. clause_keywords API(키워드 중심, group 없이)가
// sentiment 필터 + count 내림차순 정렬 + limit을 이미 지원하므로, 긍정/부정 각각
// sentiment=positive|negative + limit=표시개수로 실제 순위를 받아온다(백엔드 변경 없음).

// 한 화면에 보여줄 순위 개수 — 그대로 서버 limit으로 전달(백엔드 limit 1~1000 지원).
const PAGE_SIZE_OPTIONS = [10, 30, 50, 100] as const;
const DEFAULT_PAGE_SIZE = 10;

// 10행이 보이는 본문 높이. 행 높이(약 36px)에 맞춘 근사값. N을 넘으면 스크롤.
const VISIBLE_ROWS = 10;
const ROW_HEIGHT = 36;

type RankRow = { keyword: string; count: number };

function RankTable({
  title,
  rows,
  loading,
  tone,
  icon: Icon,
}: {
  title: string;
  rows: RankRow[];
  loading: boolean;
  tone: "positive" | "negative";
  icon: typeof ThumbsUp;
}) {
  const accent =
    tone === "positive"
      ? "text-emerald-600 bg-emerald-50"
      : "text-rose-600 bg-rose-50";

  return (
    <div className="overflow-hidden rounded-2xl border border-zinc-100 bg-white shadow-sm">
      <div className="flex items-center gap-2 border-b border-zinc-50 px-4 py-3">
        <span className={`grid h-6 w-6 place-items-center rounded-lg ${accent}`}>
          <Icon className="h-3.5 w-3.5" />
        </span>
        <div className="text-[15px] font-bold text-zinc-900">{title}</div>
        <span className="text-xs text-zinc-400">{rows.length}개</span>
      </div>

      <div
        className="relative overflow-y-auto"
        style={{ maxHeight: VISIBLE_ROWS * ROW_HEIGHT }}
      >
        <table className="w-full table-fixed text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="border-b border-zinc-100 bg-zinc-50/95 text-left text-xs font-semibold text-zinc-500 backdrop-blur">
              <th className="w-14 px-4 py-2.5">순위</th>
              <th className="px-4 py-2.5">키워드</th>
              <th className="w-20 px-4 py-2.5 text-right">빈도</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {loading ? (
              <tr>
                <td colSpan={3} className="py-10 text-center">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin text-violet-400" />
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td
                  colSpan={3}
                  className="py-8 text-center text-sm text-zinc-400"
                >
                  키워드가 없습니다
                </td>
              </tr>
            ) : (
              rows.map((r, i) => (
                <tr
                  key={`${r.keyword}-${i}`}
                  className="transition-colors hover:bg-zinc-50/60"
                >
                  <td className="px-4 py-2 text-xs font-semibold text-zinc-400">
                    {i + 1}
                  </td>
                  <td className="px-4 py-2 font-medium text-zinc-800">
                    {r.keyword}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-zinc-600">
                    {r.count.toLocaleString()}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function KeywordSentimentRankTable() {
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE);

  // 긍정/부정 각각 빈도순 상위 pageSize개. clause_keywords(키워드 중심) API에
  // sentiment 필터 + limit을 넘기면 ORDER BY count DESC로 순위가 내려온다.
  const positive = useBuildVersion("clause_keywords", undefined, {
    sentiment: "positive",
    limit: pageSize,
  }) as { data: KeywordBuild | undefined; isLoading: boolean };
  const negative = useBuildVersion("clause_keywords", undefined, {
    sentiment: "negative",
    limit: pageSize,
  }) as { data: KeywordBuild | undefined; isLoading: boolean };

  const posRows: RankRow[] = (positive.data?.items ?? []).map((it) => ({
    keyword: it.keyword,
    count: it.count,
  }));
  const negRows: RankRow[] = (negative.data?.items ?? []).map((it) => ({
    keyword: it.keyword,
    count: it.count,
  }));

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-2">
        <div className="text-sm font-bold text-zinc-900">키워드 순위</div>
        <select
          value={pageSize}
          onChange={(e) => setPageSize(Number(e.target.value))}
          title="순위 표시 개수"
          className="h-7 rounded-lg border border-zinc-200 px-2 text-xs text-zinc-600 outline-none focus:border-violet-400"
        >
          {PAGE_SIZE_OPTIONS.map((n) => (
            <option key={n} value={n}>
              상위 {n}개
            </option>
          ))}
        </select>
      </div>
      <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
        <RankTable
          title="긍정 키워드"
          rows={posRows}
          loading={positive.isLoading}
          tone="positive"
          icon={ThumbsUp}
        />
        <RankTable
          title="부정 키워드"
          rows={negRows}
          loading={negative.isLoading}
          tone="negative"
          icon={ThumbsDown}
        />
      </div>
    </div>
  );
}
