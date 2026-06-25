import { useState } from "react";
import { Loader2, ThumbsDown, ThumbsUp } from "lucide-react";
import { useBuildVersion } from "../hooks/build.query";
import type { KeywordBuild } from "../models/build";
import type { RefineMode } from "./KeywordRefineDialog";

// 키워드 분석 탭의 "키워드 추출 결과" 카드 위에 놓는 긍/부정 키워드 순위 표.
// 왼쪽=긍정, 오른쪽=부정. 컬럼: 순위 / 키워드 / 빈도. 빈도 내림차순 상위 N을 보여주되
// 10행을 넘으면 표 본문만 스크롤한다(헤더는 sticky 고정).
//
// silverone 2026-06-24 — MOCK 제거. clause_keywords API(키워드 중심, group 없이)가
// sentiment 필터 + count 내림차순 정렬 + limit을 이미 지원하므로, 긍정/부정 각각
// sentiment=positive|negative + limit=표시개수로 실제 순위를 받아온다(백엔드 변경 없음).
// 표시 개수는 표마다 독립 토글 버튼(10/30/50/100)으로 고른다.

// 표마다 고를 수 있는 상위 N — 그대로 서버 limit으로 전달(백엔드 limit 1~1000 지원).
const PAGE_SIZE_OPTIONS = [10, 30, 50, 100] as const;
const DEFAULT_PAGE_SIZE = 10;

// 10행이 보이는 본문 높이. 행 높이(약 36px)에 맞춘 근사값. N을 넘으면 스크롤.
const VISIBLE_ROWS = 10;
const ROW_HEIGHT = 36;

function RankTable({
  title,
  sentiment,
  tone,
  icon: Icon,
  onRefine,
}: {
  title: string;
  sentiment: "positive" | "negative";
  tone: "positive" | "negative";
  icon: typeof ThumbsUp;
  onRefine?: (keyword: string, mode: RefineMode) => void;
}) {
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE);

  // 빈도순(ORDER BY count DESC) 상위 pageSize개. clause_keywords(키워드 중심) API에
  // sentiment 필터 + limit을 넘기면 순위가 내려온다.
  const { data, isLoading } = useBuildVersion("clause_keywords", undefined, {
    sentiment,
    limit: pageSize,
  }) as { data: KeywordBuild | undefined; isLoading: boolean };

  const rows = (data?.items ?? []).map((it) => ({
    keyword: it.keyword,
    count: it.count,
  }));

  const accent =
    tone === "positive"
      ? "text-emerald-600 bg-emerald-50"
      : "text-rose-600 bg-rose-50";

  return (
    <div className="overflow-hidden rounded-2xl border border-zinc-100 bg-white shadow-sm">
      <div className="flex flex-wrap items-center gap-2 border-b border-zinc-50 px-4 py-3">
        <span className={`grid h-6 w-6 place-items-center rounded-lg ${accent}`}>
          <Icon className="h-3.5 w-3.5" />
        </span>
        <div className="text-[15px] font-bold text-zinc-900">{title}</div>
        <span className="text-xs text-zinc-400">{rows.length}개</span>
        {/* 표시 개수 토글 버튼 (표마다 독립) */}
        <div className="ml-auto flex items-center gap-1 rounded-lg bg-zinc-100 p-0.5">
          {PAGE_SIZE_OPTIONS.map((n) => {
            const on = pageSize === n;
            return (
              <button
                key={n}
                type="button"
                onClick={() => setPageSize(n)}
                aria-pressed={on}
                title={`상위 ${n}개 보기`}
                className={`rounded-md px-2 py-0.5 text-xs font-semibold transition-colors ${
                  on
                    ? "bg-white text-zinc-900 shadow-sm"
                    : "text-zinc-500 hover:text-zinc-800"
                }`}
              >
                {n}
              </button>
            );
          })}
        </div>
      </div>

      <div
        className="relative overflow-y-auto"
        style={{ maxHeight: VISIBLE_ROWS * ROW_HEIGHT }}
      >
        <table className="w-full table-fixed text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="border-b border-zinc-100 bg-zinc-50/95 text-left text-xs font-semibold text-zinc-500 backdrop-blur">
              <th className="w-12 px-4 py-2.5">순위</th>
              <th className="px-4 py-2.5">키워드</th>
              <th className="w-16 px-4 py-2.5 text-right">빈도</th>
              {onRefine && <th className="w-28 px-2 py-2.5 text-right">정제</th>}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {isLoading ? (
              <tr>
                <td colSpan={onRefine ? 4 : 3} className="py-10 text-center">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin text-violet-400" />
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td
                  colSpan={onRefine ? 4 : 3}
                  className="py-8 text-center text-sm text-zinc-400"
                >
                  키워드가 없습니다
                </td>
              </tr>
            ) : (
              rows.map((r, i) => (
                <tr
                  key={`${r.keyword}-${i}`}
                  className="group transition-colors hover:bg-zinc-50/60"
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
                  {onRefine && (
                    <td className="px-2 py-2 text-right whitespace-nowrap">
                      <button
                        type="button"
                        onClick={() => onRefine(r.keyword, "synonym")}
                        className="rounded-md px-1.5 py-0.5 text-xs font-medium text-violet-600 opacity-0 transition-opacity hover:bg-violet-50 group-hover:opacity-100"
                        title="대표어 지정(병합)"
                      >
                        병합
                      </button>
                      <button
                        type="button"
                        onClick={() => onRefine(r.keyword, "exclude")}
                        className="rounded-md px-1.5 py-0.5 text-xs font-medium text-rose-600 opacity-0 transition-opacity hover:bg-rose-50 group-hover:opacity-100"
                        title="키워드 제외"
                      >
                        제외
                      </button>
                    </td>
                  )}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function KeywordSentimentRankTable({
  onRefine,
}: {
  onRefine?: (keyword: string, mode: RefineMode) => void;
}) {
  return (
    <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
      <RankTable
        title="긍정 키워드"
        sentiment="positive"
        tone="positive"
        icon={ThumbsUp}
        onRefine={onRefine}
      />
      <RankTable
        title="부정 키워드"
        sentiment="negative"
        tone="negative"
        icon={ThumbsDown}
        onRefine={onRefine}
      />
    </div>
  );
}
