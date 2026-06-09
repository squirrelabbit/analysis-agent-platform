import { useMemo, useState } from "react";
import {
  ChevronRight,
  Hash,
  Layers,
  Percent,
  Search,
  Star,
  ThumbsDown,
  ThumbsUp,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { StatCard } from "@/components/common/cards/StatCard";
import { Badge } from "@/components/ui/badge";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { aspectLabelOf } from "@/features/taxonomy/models";
import {
  DataTable,
  ExpandableTextCell,
  FilterPills,
  type Column,
} from "../DataTable";
import { BuildTabEmpty, isBuildRunning } from "../BuildStatusMeta";
import { useKeyword } from "../../hooks/keyword.query";
import type {
  KeywordRankItem,
  KeywordTableItem,
  Sentiment,
} from "../../models/keyword";

const SENTIMENT_LABELS: Record<Sentiment, string> = {
  positive: "긍정",
  neutral: "중립",
  negative: "부정",
};

// 키워드 배지의 감성별 배경·글자색.
const SENTIMENT_BADGE: Record<Sentiment, string> = {
  positive: "bg-emerald-50 text-emerald-600",
  neutral: "bg-zinc-100 text-zinc-500",
  negative: "bg-red-50 text-red-600",
};

// Aspect 키워드 테이블의 감성 점 색.
const SENTIMENT_DOT: Record<Sentiment, string> = {
  positive: "bg-emerald-500",
  neutral: "bg-zinc-300",
  negative: "bg-red-500",
};

// 감성 파이(conic-gradient) 색.
const PIE_POSITIVE = "#10b981";
const PIE_NEUTRAL = "#d4d4d8";
const PIE_NEGATIVE = "#ef4444";

const SENTIMENT_FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  { label: "긍정", value: "positive" },
  { label: "중립", value: "neutral" },
  { label: "부정", value: "negative" },
];

const PAGE_SIZE = 10;

// Aspect 우측 패널 키워드 표는 상위 N개만 (좌측 목록 높이에 맞춤).
// 나머지는 아래 "키워드 상세" 표에서 전체 확인.
const ASPECT_KEYWORD_LIMIT = 5;

// 칭찬/불만 랭킹 한 컬럼. tone에 따라 색만 다르다.
function RankList({
  title,
  icon,
  items,
  tone,
}: {
  title: string;
  icon: React.ReactNode;
  items: KeywordRankItem[];
  tone: "positive" | "negative";
}) {
  const max = items[0]?.count || 1;
  const bar = tone === "positive" ? "bg-emerald-500" : "bg-red-500";
  const rank = tone === "positive" ? "text-emerald-600" : "text-red-500";
  return (
    <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
      <div className="mb-3 flex items-center gap-2">
        <span
          className={cn(
            "grid h-6 w-6 place-items-center rounded-md text-xs",
            tone === "positive"
              ? "bg-emerald-100 text-emerald-600"
              : "bg-red-100 text-red-600",
          )}
        >
          {icon}
        </span>
        <div className="text-[15px] font-bold">{title}</div>
      </div>
      <div className="flex flex-col gap-2.5">
        {items.map((it, i) => (
          <div
            key={it.term}
            className="grid grid-cols-[20px_64px_1fr_auto] items-center gap-2.5"
          >
            <span className={cn("text-xs font-bold", rank)}>{i + 1}</span>
            <span className="truncate text-xs font-bold text-zinc-700">
              {it.term}
            </span>
            <span className="h-2 overflow-hidden rounded-full bg-zinc-100">
              <span
                className={cn("block h-full rounded-full", bar)}
                style={{ width: `${(it.count / max) * 100}%` }}
              />
            </span>
            <span className="text-xs font-semibold tabular-nums text-zinc-500">
              {it.count.toLocaleString()}건
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function KeywordTab() {
  const { data, isLoading } = useKeyword();
  // taxonomy 조회 실패해도 aspectLabelOf가 key로 fallback하므로 화면은 동작.
  const { data: taxonomy } = useTaxonomy();

  const [activeAspect, setActiveAspect] = useState<string | null>(null);
  const [sentiment, setSentiment] = useState<string>("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  const { summary, positiveTop, negativeTop, aspects, items, status } = data;

  // 검색 + 감성 필터(클라이언트). 표는 이미 출현수 내림차순 정렬된 items 기준.
  const filtered = useMemo(
    () =>
      items.filter(
        (it) =>
          (!sentiment || it.sentiment === sentiment) &&
          (!search || it.term.includes(search.trim())),
      ),
    [items, sentiment, search],
  );

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20 text-sm text-zinc-400">
        결과를 불러오는 중…
      </div>
    );
  }

  // 키워드 분석은 절 라벨링 결과에 의존. summary 없으면 안내.
  if (!summary) {
    return isBuildRunning(status) ? (
      <div className="rounded-2xl border border-blue-200 bg-blue-50/70 p-4 text-sm font-medium text-blue-700">
        절 라벨링이 진행 중입니다. 완료되면 키워드 분석이 표시됩니다.
      </div>
    ) : (
      <BuildTabEmpty type="clause_label" status={status} />
    );
  }

  // 선택 aspect (기본값 = 문장수 1위).
  const selected =
    aspects.find((a) => a.aspectKey === activeAspect) ?? aspects[0];
  const maxSentence = Math.max(...aspects.map((a) => a.sentenceCount), 1);

  // 우측 패널 감성 우세 판정.
  const dominant = (["positive", "negative", "neutral"] as Sentiment[]).reduce(
    (best, s) => (selected.sentiment[s] > selected.sentiment[best] ? s : best),
    "neutral" as Sentiment,
  );

  // 감성 파이(conic-gradient) — 긍/중/부 비율 누적.
  const { positive: posPct, neutral: neuPct, negative: negPct } =
    selected.sentiment;
  const pieGradient = `conic-gradient(${PIE_POSITIVE} 0 ${posPct}%, ${PIE_NEUTRAL} ${posPct}% ${posPct + neuPct}%, ${PIE_NEGATIVE} ${posPct + neuPct}% 100%)`;

  // 우측 키워드 표는 상위 N개만. 초과분은 "외 N개"로 안내.
  const topKeywords = selected.keywords.slice(0, ASPECT_KEYWORD_LIMIT);
  const moreKeywordCount = selected.keywords.length - topKeywords.length;

  // 표 페이징(클라이언트).
  const totalCount = filtered.length;
  const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE));
  const pageItems = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const columns: Column<KeywordTableItem>[] = [
    {
      header: "키워드",
      headerClassName: "w-32",
      cell: (it) => (
        <td className="px-4 py-3 font-bold text-zinc-800">{it.term}</td>
      ),
    },
    {
      // 활성 정렬 컬럼: 출현수 내림차순.
      header: (
        <span className="inline-flex items-center gap-1 text-violet-600">
          출현수 <span className="text-[10px]">▼</span>
        </span>
      ),
      headerClassName: "w-24 text-right",
      cell: (it) => (
        <td className="px-4 py-3 text-right font-bold tabular-nums text-zinc-800">
          {it.count.toLocaleString()}
        </td>
      ),
    },
    {
      header: "문서수",
      headerClassName: "w-20 text-right",
      cell: (it) => (
        <td className="px-4 py-3 text-right tabular-nums text-zinc-500">
          {it.docCount.toLocaleString()}
        </td>
      ),
    },
    {
      header: "대표 감성",
      headerClassName: "w-28",
      cell: (it) => (
        <td className="px-4 py-3">
          <Badge className={SENTIMENT_BADGE[it.sentiment]}>
            {SENTIMENT_LABELS[it.sentiment]} {it.sentimentPercent}%
          </Badge>
        </td>
      ),
    },
    {
      header: "연관 Aspect",
      headerClassName: "w-32",
      cell: (it) => (
        <td className="px-4 py-3 text-xs text-zinc-500">
          {aspectLabelOf(taxonomy, it.aspectKey)}
        </td>
      ),
    },
    {
      header: "대표 문장",
      cell: (it) => <ExpandableTextCell text={it.representativeSentence} />,
    },
  ];

  return (
    <div className="space-y-5">
      {/* 메타 */}

      {/* 요약 통계 */}
      <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
        <StatCard
          value={summary.totalOccurrences.toLocaleString()}
          label="총 키워드 추출"
          icon={Hash}
          tone="neutral"
        />
        <StatCard
          value={summary.uniqueCount.toLocaleString()}
          label="고유 키워드 수"
          icon={Layers}
          tone="blue"
        />
        <StatCard
          value={summary.topTerms.join(" · ") || "-"}
          label="최다 출현 키워드"
          icon={Star}
          tone="muted"
        />
        <StatCard
          value={`${summary.docCoveragePercent}%`}
          label="문서 커버리지"
          icon={Percent}
          tone="ok"
          valueColor="text-emerald-600"
        />
      </div>

      {/* Aspect별 키워드 — 좌 목록 / 우 드릴다운 (ClauseTab 패턴) */}
      <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
        <div className="mb-1 flex items-center justify-between">
          <div className="text-[15px] font-bold">Aspect별 키워드</div>
          <div className="text-[11px] font-semibold text-zinc-400">
            막대 = 문장 수
          </div>
        </div>
        <p className="mb-3 text-xs font-medium text-zinc-400">
          절 라벨링 aspect · 항목을 누르면 오른쪽에 해당 주제의 키워드가
          표시됩니다
        </p>

        <div className="grid grid-cols-1 gap-7 md:grid-cols-[minmax(240px,1fr)_1px_minmax(240px,0.9fr)]">
          {/* 좌: aspect 목록 */}
          <div className="flex flex-col gap-1 mt-2">
            {aspects.map((a) => {
              const sel = a.aspectKey === selected.aspectKey;
              return (
                <button
                  key={a.aspectKey}
                  type="button"
                  onClick={() => setActiveAspect(a.aspectKey)}
                  className={cn(
                    "grid grid-cols-[108px_1fr_auto_16px] items-center gap-2.5 rounded-xl border-l-2 px-2 py-2 text-left transition-colors",
                    sel
                      ? "border-violet-500 bg-violet-50"
                      : "border-transparent hover:cursor-pointer hover:bg-zinc-50",
                  )}
                >
                  <span
                    className={cn(
                      "truncate text-right text-xs font-semibold",
                      sel ? "text-violet-700" : "text-zinc-600",
                    )}
                  >
                    {aspectLabelOf(taxonomy, a.aspectKey)}
                  </span>
                  <span className="h-2.5 overflow-hidden rounded-full bg-zinc-100">
                    <span
                      className={cn(
                        "block h-full rounded-full bg-linear-to-r",
                        sel
                          ? "from-violet-600 to-violet-400"
                          : "from-blue-500 to-blue-400",
                      )}
                      style={{
                        width: `${(a.sentenceCount / maxSentence) * 100}%`,
                      }}
                    />
                  </span>
                  <span className="min-w-7 text-right text-xs font-bold tabular-nums text-zinc-800">
                    {a.sentenceCount}
                  </span>
                  <ChevronRight
                    className={cn(
                      "h-3.5 w-3.5",
                      sel ? "text-violet-600" : "text-zinc-300",
                    )}
                  />
                </button>
              );
            })}
          </div>

          {/* 구분선 */}
          <div className="hidden self-stretch bg-zinc-100 md:block" />

          {/* 우: 선택 aspect의 키워드 — 파이 + 빈도순 테이블 (섹션 제목 없이 좌측 높이에 맞춤) */}
          <div className="flex flex-col gap-3">
            <div className="flex items-center gap-2">
              <span className="h-2 w-2 rounded-full bg-violet-600" />
              <span className="font-extrabold text-violet-700">
                {aspectLabelOf(taxonomy, selected.aspectKey)}
              </span>
              <span
                className={cn(
                  "rounded-md px-1.5 py-0.5 text-[10px] font-bold",
                  SENTIMENT_BADGE[dominant],
                )}
              >
                {SENTIMENT_LABELS[dominant]} 우세
              </span>
              <span className="ml-auto text-xs font-semibold text-zinc-400">
                {selected.sentenceCount}문장
              </span>
            </div>

            {/* 감성 파이 + 범례 (제목 없음) */}
            <div className="flex items-center gap-6 rounded-2xl bg-zinc-50/70 px-5 py-4">
              <div className="relative h-25 w-25 shrink-0">
                <div
                  className="h-25 w-25 rounded-full"
                  style={{ background: pieGradient }}
                />
                <div className="absolute inset-[13px] grid place-items-center rounded-full bg-white text-center shadow-sm">
                  <div>
                    <div className="text-2xl font-extrabold leading-none tabular-nums text-zinc-900">
                      {selected.sentenceCount}
                    </div>
                    <div className="mt-0.5 text-[10px] font-semibold text-zinc-400">
                      문장
                    </div>
                  </div>
                </div>
              </div>
              <div className="flex flex-1 flex-col gap-3 text-[13px]">
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full bg-emerald-500" />
                  <span className="font-semibold text-zinc-600">긍정</span>
                  <span className="ml-auto text-lg font-extrabold leading-none tabular-nums text-emerald-600">
                    {posPct}
                    <span className="text-xs">%</span>
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full bg-zinc-300" />
                  <span className="font-semibold text-zinc-600">중립</span>
                  <span className="ml-auto text-lg font-extrabold leading-none tabular-nums text-zinc-400">
                    {neuPct}
                    <span className="text-xs">%</span>
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full bg-red-500" />
                  <span className="font-semibold text-zinc-600">부정</span>
                  <span className="ml-auto text-lg font-extrabold leading-none tabular-nums text-red-500">
                    {negPct}
                    <span className="text-xs">%</span>
                  </span>
                </div>
              </div>
            </div>

            {/* 키워드 테이블 (제목 없음, 빈도순) */}
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-zinc-100 text-[11px] text-zinc-400">
                  <th className="pb-1.5 text-left font-semibold">키워드</th>
                  <th className="pb-1.5 text-right font-semibold">빈도</th>
                  <th className="pb-1.5 text-right font-semibold">감성</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-50">
                {topKeywords.map((k) => (
                  <tr key={k.term}>
                    <td className="py-1.5 font-semibold text-zinc-700">
                      {k.term}
                    </td>
                    <td className="py-1.5 text-right font-bold tabular-nums text-zinc-800">
                      {k.count}
                    </td>
                    <td className="py-1.5 text-right">
                      <span
                        className={cn(
                          "inline-block h-1.5 w-1.5 rounded-full align-middle",
                          SENTIMENT_DOT[k.sentiment],
                        )}
                      />{" "}
                      <span className="text-zinc-400">
                        {SENTIMENT_LABELS[k.sentiment]}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {moreKeywordCount > 0 && (
              <div className="text-[11px] font-medium text-zinc-400">
                외 {moreKeywordCount}개 · 아래 키워드 상세 표에서 전체 확인
              </div>
            )}
          </div>
        </div>
      </div>

      {/* 긍정 / 부정 키워드 Top 5 */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <RankList
          title="긍정 키워드 Top 5"
          icon={<ThumbsUp className="h-3.5 w-3.5" strokeWidth={2} />}
          items={positiveTop}
          tone="positive"
        />
        <RankList
          title="부정 키워드 Top 5"
          icon={<ThumbsDown className="h-3.5 w-3.5" strokeWidth={2} />}
          items={negativeTop}
          tone="negative"
        />
      </div>

      {/* 상세 테이블 — 출현수 내림차순 */}
      <DataTable
        columns={columns}
        items={pageItems}
        rowKey={(it) => it.term}
        title="키워드 상세"
        toolbar={
          <>
            <div className="relative">
              <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-400" />
              <input
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value);
                  setPage(1);
                }}
                placeholder="키워드 검색…"
                className="h-7 w-40 rounded-lg border border-zinc-200 pl-7 pr-2.5 text-xs outline-none focus:border-violet-400"
              />
            </div>
            <FilterPills
              options={SENTIMENT_FILTER_OPTIONS}
              value={sentiment}
              onChange={(v) => {
                setSentiment(v);
                setPage(1);
              }}
            />
          </>
        }
        page={page}
        totalPages={totalPages}
        totalCount={totalCount}
        onPageChange={setPage}
      />
    </div>
  );
}
