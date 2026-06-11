import { useEffect, useState } from "react";
import {
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
import { BarTrack } from "@/components/common/charts";
import { Badge } from "@/components/ui/badge";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { aspectLabelOf } from "@/features/taxonomy/models";
import {
  DataTable,
  ExpandableTextCell,
  FilterPills,
  type Column,
} from "../DataTable";
import {
  BuildMetaBar,
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  isBuildRunning,
} from "../BuildStatusMeta";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type {
  AspectSentimentKeywords,
  KeywordBuild,
  KeywordItem,
} from "../../models/build";
import {
  SENTIMENT_BADGE,
  SENTIMENT_COLORS,
  SENTIMENT_FILTER_OPTIONS,
  SENTIMENT_LABELS,
  type Sentiment,
} from "../../constants/sentiment";
import { useBuildVersion } from "../../hooks/build.query";

const PAGE_SIZE = 10;

// Aspect 우측 표는 상위 N개만. 나머지는 아래 "키워드 상세" 표에서 전체 확인.
const ASPECT_KEYWORD_LIMIT = 10;

const toSentiment = (raw: string): Sentiment =>
  raw === "positive" || raw === "negative" ? raw : "neutral";

// dominant_sentiment_ratio 스케일이 0~1인지 0~100인지 불명확(확인 필요). 1 이하면 ×100.
const toPercent = (ratio: number): number =>
  Math.round(ratio <= 1 ? ratio * 100 : ratio);

// 건수 최다 aspect key (selected_aspect가 비었을 때 fallback).
const topAspectKey = (aspect: Record<string, number>): string => {
  let best = "";
  let bestN = -1;
  for (const [k, v] of Object.entries(aspect)) {
    if (v > bestN) {
      best = k;
      bestN = v;
    }
  }
  return best;
};

// 워드클라우드/표용 키워드. 선택 aspect의 긍/부정 리스트를 term 기준으로 합산.
// 같은 키워드가 양쪽에 등장할 수 있어 합치고, 우세 감성(동률이면 중립)을 정한다.
type AspectKeyword = {
  term: string;
  count: number; // 긍정+부정 합산
  sentiment: Sentiment; // 우세 감성
  positiveCount: number;
  negativeCount: number;
};

const dedupeAspectKeywords = (
  group?: AspectSentimentKeywords,
): AspectKeyword[] => {
  const byTerm = new Map<string, { positive: number; negative: number }>();
  const acc = (
    list: { keyword: string; count: number }[],
    key: "positive" | "negative",
  ) => {
    for (const k of list) {
      const e = byTerm.get(k.keyword) ?? { positive: 0, negative: 0 };
      e[key] += k.count;
      byTerm.set(k.keyword, e);
    }
  };
  acc(group?.positive ?? [], "positive");
  acc(group?.negative ?? [], "negative");
  return [...byTerm.entries()]
    .map(([term, e]) => ({
      term,
      count: e.positive + e.negative,
      sentiment:
        e.positive === e.negative
          ? ("neutral" as const)
          : e.positive > e.negative
            ? ("positive" as const)
            : ("negative" as const),
      positiveCount: e.positive,
      negativeCount: e.negative,
    }))
    .sort((a, b) => b.count - a.count);
};

// ── Aspect 워드클라우드 ───────────────────────────────────────
// 선택 aspect의 키워드를 글자 크기 = 빈도로 배치. 색은 보기 모드로 결정.
const CLOUD_MAX_WORDS = 18;
const CLOUD_MIN_PX = 15;
const CLOUD_MAX_PX = 44;
// 가운데 줄에 큰 단어가 모이고 위·아래로 작은 단어가 퍼지도록 한 행별 단어 수(top→bottom).
const CLOUD_ROW_PLAN = [3, 4, 4, 2];

type CloudInput = { term: string; count: number; sentiment: Sentiment };
type CloudWord = CloudInput & { px: number };

// 빈도 내림차순 키워드를 타원형 실루엣 행 배열로 변환.
// 중앙 행부터 큰 단어를 채워 가운데가 크고 위·아래로 작아지는 덩어리를 만든다.
function buildCloud(keywords: CloudInput[]): CloudWord[][] {
  const words = [...keywords]
    .sort((a, b) => b.count - a.count)
    .slice(0, CLOUD_MAX_WORDS);
  if (words.length === 0) return [];

  // sqrt 스케일로 빈도 → 글자 크기(px). 큰 차이를 완만하게.
  const counts = words.map((w) => w.count);
  const sqMax = Math.sqrt(Math.max(...counts));
  const sqMin = Math.sqrt(Math.min(...counts));
  const sized: CloudWord[] = words.map((w) => {
    const t =
      sqMax === sqMin ? 1 : (Math.sqrt(w.count) - sqMin) / (sqMax - sqMin);
    return {
      ...w,
      px: Math.round(CLOUD_MIN_PX + t * (CLOUD_MAX_PX - CLOUD_MIN_PX)),
    };
  });

  // 중앙 행부터 채우는 순서 (가운데와의 거리 오름차순).
  const rowCount = CLOUD_ROW_PLAN.length;
  const mid = (rowCount - 1) / 2;
  const fillOrder = [...Array(rowCount).keys()].sort(
    (a, b) => Math.abs(a - mid) - Math.abs(b - mid),
  );

  const rows: CloudWord[][] = Array.from({ length: rowCount }, () => []);
  let idx = 0;
  for (const row of fillOrder) {
    const slice = sized.slice(idx, idx + CLOUD_ROW_PLAN[row]);
    idx += slice.length;
    // 행 안에서도 큰 단어가 가운데 오도록 center-out 배치.
    const centered: CloudWord[] = [];
    slice.forEach((w, i) =>
      i % 2 === 0 ? centered.push(w) : centered.unshift(w),
    );
    rows[row] = centered;
  }
  return rows.filter((r) => r.length > 0);
}

// 칭찬/불만 랭킹 한 컬럼. tone에 따라 색만 다르다.
function RankList({
  title,
  icon,
  items,
  tone,
}: {
  title: string;
  icon: React.ReactNode;
  items: { term: string; count: number }[];
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
            <BarTrack
              className="h-2"
              percent={(it.count / max) * 100}
              fillClassName={bar}
            />
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
  // 상단 Aspect 칩(워드클라우드·상위표)은 프론트 전용 — 처음 받아온 summary를 필터링만.
  const [chipAspect, setChipAspect] = useState<string | null>(null);
  // 하단 상세표의 Aspect 필터 — 서버 파라미터(aspect)로 호출.
  const [activeAspect, setActiveAspect] = useState<string | null>(null);
  // Aspect 워드클라우드 보기 모드. all = 단색(빈도만), sentiment = 우세 감성색.
  const [cloudMode, setCloudMode] = useState<"all" | "sentiment">("all");
  const [sentiment, setSentiment] = useState<string>("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  // 검색어 디바운스 → 서버 q (입력마다 재조회 방지). 적용 시 1페이지로.
  const [debouncedSearch, setDebouncedSearch] = useState("");
  useEffect(() => {
    const t = setTimeout(() => {
      setDebouncedSearch(search.trim());
      setPage(1);
    }, 300);
    return () => clearTimeout(t);
  }, [search]);

  // 서버 파라미터는 상세 표 전용: aspect(표 필터)/sentiment/q/limit/offset.
  // 상단 칩·워드클라우드는 summary를 프론트에서 필터링하므로 여기 영향 없음.
  const { data, isLoading, isPlaceholderData } = useBuildVersion(
    "clause_keywords",
    undefined,
    {
      aspect: activeAspect ?? undefined,
      sentiment: sentiment || undefined,
      q: debouncedSearch || undefined,
      limit: PAGE_SIZE,
      offset: (page - 1) * PAGE_SIZE,
    },
  ) as {
    data: KeywordBuild | undefined;
    isLoading: boolean;
    isPlaceholderData: boolean;
  };
  const {
    summary,
    items = [],
    applied,
    status,
    progress,
    durationSeconds,
    pagination,
  } = data || {};
  // taxonomy 조회 실패해도 aspectLabelOf가 key로 fallback하므로 화면은 동작.
  const { data: taxonomy } = useTaxonomy();

  if (isLoading) return  <BuildTabLoading />

  if (!summary) {
    return isBuildRunning(status) ? (
      <BuildRunningBanner
        status={status}
        progress={progress}
        hasPrevious={false}
      />
    ) : (
      <BuildTabEmpty type="clause_keywords" status={status} />
    );
  }

  // 선택 칩 = 프론트 상태(없으면 서버 selected_aspect, 그것도 없으면 문장수 1위).
  const selectedKey =
    chipAspect || summary.selectedAspect || topAspectKey(summary.aspect);

  // 칩 목록 (문장수 내림차순).
  const chips = Object.entries(summary.aspect)
    .map(([aspectKey, sentenceCount]) => ({ aspectKey, sentenceCount }))
    .sort((a, b) => b.sentenceCount - a.sentenceCount);

  // 선택 aspect 키워드 (긍/부정 term 합산) — 워드클라우드 + 표.
  const aspectKeywords = dedupeAspectKeywords(
    summary.aspectSentimentKeywords[selectedKey],
  );
  const aspectCloudRows = buildCloud(aspectKeywords);
  const topKeywords = aspectKeywords.slice(0, ASPECT_KEYWORD_LIMIT);
  const moreKeywordCount = aspectKeywords.length - topKeywords.length;

  // 칭찬/불만 Top — 서버가 이미 N개씩 줌(슬라이스 없음). count는 문자열일 수 있어 Number 변환.
  const positiveTop = summary.topKeywordsPositive.map((k) => ({
    term: String(k.keyword ?? ""),
    count: Number(k.count ?? 0),
  }));
  const negativeTop = summary.topKeywordsNegative.map((k) => ({
    term: String(k.keyword ?? ""),
    count: Number(k.count ?? 0),
  }));

  // 상세 표 = 서버 페이지(KeywordItem). pagination.total = (필터 적용) 전체 건수.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE));

  // 최다 출현 키워드 — 전역 필드가 없어 현재 페이지 상위로 근사 (page 1·무필터에서 정확).
  const topTerms = items.slice(0, 2).map((it) => it.keyword);

  const columns: Column<KeywordItem>[] = [
    {
      header: "키워드",
      headerClassName: "w-32",
      cell: (it) => (
        <td className="px-4 py-3 font-bold text-zinc-800">{it.keyword}</td>
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
          {it.documentCount.toLocaleString()}
        </td>
      ),
    },
    {
      header: "대표 감성",
      headerClassName: "w-28",
      cell: (it) => {
        const s = toSentiment(it.dominantSentiment);
        return (
          <td className="px-4 py-3">
            <Badge className={SENTIMENT_BADGE[s]}>
              {SENTIMENT_LABELS[s]} {toPercent(it.dominantSentimentRatio)}%
            </Badge>
          </td>
        );
      },
    },
    {
      header: "연관 Aspect",
      headerClassName: "w-32",
      cell: (it) => (
        <td className="px-4 py-3 text-xs text-zinc-500">
          {aspectLabelOf(taxonomy, it.topAspect)}
        </td>
      ),
    },
    {
      header: "대표 문장",
      cell: (it) => <ExpandableTextCell text={it.representativeClause} />,
    },
  ];

  return (
    <div className="space-y-5">
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

      {/* Aspect별 키워드 — 상단 칩 선택 + 워드클라우드(좌)/표(우) */}
      <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
        <div className="mb-1 flex items-center justify-between">
          <div className="text-[15px] font-bold">Aspect별 키워드</div>
          <div className="text-[11px] font-semibold text-zinc-400">
            워드클라우드 = 빈도 · 표 = 긍/부정
          </div>
        </div>
        <p className="mb-3 text-xs font-medium text-zinc-400">
          주제(aspect)를 고르면 해당 주제의 키워드를 워드클라우드와 표로
          보여줍니다
        </p>

        {/* aspect 칩 선택 (문장수 내림차순) */}
        <div className="mb-4 flex flex-wrap gap-1.5">
          {chips.map((a) => {
            const sel = a.aspectKey === selectedKey;
            return (
              <button
                key={a.aspectKey}
                type="button"
                onClick={() => setChipAspect(a.aspectKey)}
                className={cn(
                  "rounded-full px-3 py-1.5 text-xs font-semibold transition-colors",
                  sel
                    ? "bg-violet-600 text-white"
                    : "bg-zinc-100 text-zinc-600 hover:cursor-pointer hover:bg-zinc-200",
                )}
              >
                {aspectLabelOf(taxonomy, a.aspectKey)}{" "}
                <span className={sel ? "opacity-70" : "text-zinc-400"}>
                  {a.sentenceCount}
                </span>
              </button>
            );
          })}
        </div>

        {/* 본문: 워드클라우드(좌) + 표(우) */}
        <div className="grid grid-cols-1 gap-6 md:grid-cols-[1.1fr_0.9fr]">
          {/* 좌: 워드클라우드 */}
          <div className="flex flex-col gap-2">
            {/* 보기 모드 토글 + 우세 감성 배지 (주제명·문장수는 칩에 이미 노출) */}
            <div className="flex items-center gap-2">
              <div className="flex rounded-lg bg-zinc-100 p-0.5 text-[11px] font-semibold">
                <button
                  type="button"
                  onClick={() => setCloudMode("all")}
                  className={cn(
                    "rounded-md px-2.5 py-1 transition-colors",
                    cloudMode === "all"
                      ? "bg-white text-violet-700 shadow-sm"
                      : "text-zinc-500 hover:text-zinc-700",
                  )}
                >
                  전체
                </button>
                <button
                  type="button"
                  onClick={() => setCloudMode("sentiment")}
                  className={cn(
                    "rounded-md px-2.5 py-1 transition-colors",
                    cloudMode === "sentiment"
                      ? "bg-white text-violet-700 shadow-sm"
                      : "text-zinc-500 hover:text-zinc-700",
                  )}
                >
                  감성
                </button>
              </div>
              {cloudMode === "sentiment" ? (
                <div className="ml-auto flex items-center gap-2.5 text-[10px] font-semibold">
                  <span className="inline-flex items-center gap-1 text-emerald-600">
                    <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                    긍정
                  </span>
                  <span className="inline-flex items-center gap-1 text-red-500">
                    <span className="h-1.5 w-1.5 rounded-full bg-red-500" />
                    부정
                  </span>
                </div>
              ) : (
                <span className="ml-auto text-[10px] font-medium text-zinc-400">
                  글자 크기 = 빈도
                </span>
              )}
            </div>

            {/* 워드클라우드 — 글자 크기 = 빈도. 색은 보기 모드로 결정 */}
            <div className="flex min-h-44 flex-1 flex-col items-center justify-center gap-y-1.5 rounded-2xl bg-zinc-50/70 px-5 py-8 leading-none">
              {aspectCloudRows.length === 0 ? (
                <span className="text-xs text-zinc-400">키워드가 없습니다</span>
              ) : (
                aspectCloudRows.map((row, ri) => (
                  <div
                    key={ri}
                    className="flex flex-wrap items-baseline justify-center gap-x-4"
                  >
                    {row.map((w) => (
                      <span
                        key={w.term}
                        className={cn(
                          cloudMode === "all" && "text-zinc-600",
                          w.px >= 30
                            ? "font-extrabold"
                            : w.px >= 20
                              ? "font-bold"
                              : "font-semibold",
                        )}
                        style={{
                          fontSize: `${w.px}px`,
                          color:
                            cloudMode === "sentiment"
                              ? SENTIMENT_COLORS[w.sentiment]
                              : undefined,
                        }}
                      >
                        {w.term}
                      </span>
                    ))}
                  </div>
                ))
              )}
            </div>
          </div>

          {/* 우: 키워드 표 (빈도 + 긍/부정) */}
          <div className="flex flex-col">
            {/* 키워드 테이블 — 전체 빈도 + 긍정/부정 분해 (빈도 내림차순) */}
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-zinc-100 text-[11px] text-zinc-400">
                  <th className="pb-1.5 text-left font-semibold">키워드</th>
                  <th className="pb-1.5 text-right font-semibold">빈도</th>
                  <th className="pb-1.5 text-right font-semibold text-emerald-600">
                    긍정
                  </th>
                  <th className="pb-1.5 text-right font-semibold text-red-500">
                    부정
                  </th>
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
                    <td className="py-1.5 text-right tabular-nums">
                      {k.positiveCount > 0 ? (
                        <span className="font-semibold text-emerald-600">
                          {k.positiveCount}
                        </span>
                      ) : (
                        <span className="text-zinc-300">·</span>
                      )}
                    </td>
                    <td className="py-1.5 text-right tabular-nums">
                      {k.negativeCount > 0 ? (
                        <span className="font-semibold text-red-500">
                          {k.negativeCount}
                        </span>
                      ) : (
                        <span className="text-zinc-300">·</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {moreKeywordCount > 0 && (
              <div className="mt-2 text-[11px] font-medium text-zinc-400">
                외 {moreKeywordCount}개 · 아래 키워드 상세 표에서 전체 확인
              </div>
            )}
          </div>
        </div>
      </div>

      {/* 긍정 / 부정 키워드 Top */}
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

      {/* 상세 테이블 — 서버 필터/검색/페이징 */}
      <DataTable
        columns={columns}
        items={items}
        rowKey={(it) => it.keyword}
        title="키워드 상세"
        loading={isPlaceholderData}
        toolbar={
          <>
            <div className="relative">
              <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-400" />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="키워드·문장 검색…"
                className="h-7 w-40 rounded-lg border border-zinc-200 pl-7 pr-2.5 text-xs outline-none focus:border-violet-400"
              />
            </div>

            {/* Aspect 필터 — 상세 표 전용(서버 호출). 상단 칩과 독립. "전체"면 aspect 해제 */}
            <Select
              value={activeAspect ?? "all"}
              onValueChange={(v) => {
                setActiveAspect(v === "all" ? null : v);
                setPage(1);
              }}
            >
              <SelectTrigger className="h-7 w-40 text-xs">
                <SelectValue placeholder="Aspect" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">전체 Aspect</SelectItem>
                {chips.map((a) => (
                  <SelectItem key={a.aspectKey} value={a.aspectKey}>
                    {aspectLabelOf(taxonomy, a.aspectKey)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

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
