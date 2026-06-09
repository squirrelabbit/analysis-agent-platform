import { useState } from "react";
import { FileText, Check, Minus, X, Box, ChevronRight } from "lucide-react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { cn } from "@/lib/utils";
import { StatCard } from "@/components/common/cards/StatCard";
import type { ClauseBuild, ClauseItem } from "../../models/build";
import { Badge } from "@/components/ui/badge";
import { useBuildVersion } from "../../hooks/build.query";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { aspectLabelOf } from "@/features/taxonomy/models";
import {
  DataTable,
  DocIdCell,
  ExpandableTextCell,
  FilterPills,
  type Column,
} from "../DataTable";
import {
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  BuildTimerChip,
  isBuildRunning,
} from "../BuildStatusMeta";

const SENTIMENT_COLORS: Record<string, string> = {
  positive: "#10b981",
  neutral: "#a1a1aa",
  negative: "#ef4444",
};

const SENTIMENT_LABELS: Record<string, string> = {
  positive: "긍정",
  neutral: "중립",
  negative: "부정",
};

// 드릴다운 도넛/범례 순서 고정 (긍정 → 중립 → 부정)
const SENTIMENT_ORDER = ["positive", "neutral", "negative"] as const;

// 드릴다운 selector의 "전체" 항목 sentinel key (실제 aspect key와 충돌 방지).
const ALL_KEY = "__all__";

const SENTIMENT_FILTER_OPTIONS: { label: string; value: string | "" }[] = [
  { label: "전체", value: "" },
  { label: "긍정", value: "positive" },
  { label: "중립", value: "neutral" },
  { label: "부정", value: "negative" },
];

function SentimentBadge({ value }: { value: string }) {
  const map: Record<string, string> = {
    positive: "bg-emerald-50 text-emerald-600",
    neutral: "bg-zinc-100 text-zinc-500",
    negative: "bg-red-50 text-red-600",
  };
  const labels: Record<string, string> = {
    positive: "긍정",
    neutral: "중립",
    negative: "부정",
  };
  return <Badge className={map[value]}>{labels[value]}</Badge>;
}

export function ClauseTab() {
  const [filter, setFilter] = useState<string | "">("");
  const [aspectFilter, setAspectFilter] = useState<string>("all");
  const [page, setPage] = useState(1);
  // 드릴다운: 선택된 aspect key (null이면 건수 1위 aspect로 fallback)
  const [activeAspect, setActiveAspect] = useState<string | null>(null);
  const pageSize = 10;

  // 서버 페이징 + 서버 필터: 표는 서버가 필터/페이징해 준 현재 페이지(items)만 렌더.
  const { data, isLoading, isPlaceholderData } = useBuildVersion(
    "clause_label",
    undefined,
    {
      limit: pageSize,
      offset: (page - 1) * pageSize,
      aspect: aspectFilter === "all" ? undefined : aspectFilter,
      sentiment: filter || undefined,
    },
  ) as {
    data: ClauseBuild | undefined;
    isLoading: boolean;
    isPlaceholderData: boolean;
  };
  // isPlaceholderData: 페이지/필터 변경으로 새 데이터 도착 전(이전 데이터 표시 중) → 로딩.
  const tableLoading = isPlaceholderData;
  // taxonomy 조회 실패해도 aspectLabelOf가 key로 fallback하므로 화면은 동작한다.
  const { data: taxonomy } = useTaxonomy();
  const {
    summary,
    items,
    applied,
    status,
    progress,
    durationSeconds,
    pagination,
  } = data || {};

  if (isLoading) return <BuildTabLoading />;
  if (!summary) {
    return isBuildRunning(status) ? (
      <BuildRunningBanner
        status={status}
        progress={progress}
        hasPrevious={false}
      />
    ) : (
      <BuildTabEmpty type="clause_label" status={status} />
    );
  }

  const {
    sentiment: { positive, neutral, negative },
  } = summary;

  // summary.aspect는 snake_case key → 한글 label로 변환해 차트 축에 표시.
  const aspectData = Object.entries(summary.aspect)
    .sort(([, a], [, b]) => b - a)
    .map(([key, value]) => ({
      key,
      name: aspectLabelOf(taxonomy, key),
      value,
    }));

  // aspect 막대 스케일 기준 = 1위 aspect 건수 (전체는 막대 표시 안 함).
  const maxAspect = Math.max(...aspectData.map((a) => a.value), 1);

  // 드릴다운: "전체"(ALL_KEY) 또는 개별 aspect 선택. 기본값은 전체.
  const overallByName: Record<string, number> = { positive, neutral, negative };
  const selectedKey = activeAspect ?? ALL_KEY;
  const selectedAspect = summary.aspectSentiment?.[selectedKey];
  const isAll = !selectedAspect;
  const drillTotal = isAll ? summary.total : selectedAspect.total;
  // percent: 전체는 summary 기준 직접 계산(소수1자리), aspect는 백엔드 percent 사용.
  const drillData = SENTIMENT_ORDER.map((name) => {
    if (isAll) {
      const value = overallByName[name] ?? 0;
      const percent =
        summary.total > 0 ? Math.round((value / summary.total) * 1000) / 10 : 0;
      return { name, value, percent };
    }
    const s = selectedAspect.sentiment[name];
    return { name, value: s?.count ?? 0, percent: s?.percent ?? 0 };
  });
  const selectedLabel = isAll ? "전체" : aspectLabelOf(taxonomy, selectedKey);
  const selectedDesc = isAll
    ? "전체 문장의 긍정·중립·부정 구성"
    : "선택한 Aspect의 긍정·중립·부정 구성";

  // aspect 옵션은 전체 분포(summary.aspect) 기준 — 현재 페이지 items가 아니라.
  const aspectOptions = Object.keys(summary.aspect);

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산의 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  const columns: Column<ClauseItem>[] = [
    {
      header: "문서 ID",
      headerClassName: "w-30",
      cell: (item) => <DocIdCell id={item.docId} />,
    },
    {
      header: "문장",
      headerClassName: "w-80",
      cell: (item) => <ExpandableTextCell text={item.clause} />,
    },
    {
      header: "Aspect",
      headerClassName: "w-28",
      cell: (item) => (
        <td className="px-4 py-3">
          <span className="text-xs text-zinc-500">
            {aspectLabelOf(taxonomy, item.aspect)}
          </span>
        </td>
      ),
    },
    {
      header: "감성",
      headerClassName: "w-24",
      cell: (item) => (
        <td className="px-4 py-3">
          <SentimentBadge value={item.sentiment} />
        </td>
      ),
    },
  ];

  return (
    <div className="space-y-5">
      {/* 메타 */}
      <div className="flex flex-wrap items-center gap-3 text-xs text-zinc-500">
        <BuildTimerChip status={status} durationSeconds={durationSeconds} />
        <span className="h-3 w-px bg-zinc-200" />
        <span className="inline-flex items-center gap-1.5 font-medium">
          <FileText className="h-3.5 w-3.5 text-zinc-400" strokeWidth={1.8} />
          프롬프트
          <code className="rounded-md bg-violet-50 px-2 py-0.5 font-mono text-[11px] font-semibold text-violet-700">
            {applied?.promptVersion ?? "-"}
          </code>
        </span>
        <span className="h-3 w-px bg-zinc-200" />
        <span className="inline-flex items-center gap-1.5 font-medium">
          <Box className="h-3.5 w-3.5 text-zinc-400" strokeWidth={1.8} />
          모델
          {/* 표시명 우선, 없으면 raw model. raw model id는 title(tooltip)로 확인. */}
          <b className="font-bold text-zinc-800" title={applied?.model}>
            {applied?.modelDisplayName || applied?.model || "-"}
          </b>
        </span>
      </div>

      <BuildRunningBanner status={status} progress={progress} hasPrevious />

      {/* 분류 현황 */}
      <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
        <StatCard
          value={summary.total?.toLocaleString()}
          label="총 문장 수"
          icon={FileText}
          tone="neutral"
        />
        <StatCard
          value={positive?.toLocaleString()}
          label="긍정 (positive)"
          icon={Check}
          tone="ok"
          valueColor="text-emerald-600"
        />
        <StatCard
          value={neutral?.toLocaleString()}
          label="중립 (neutral)"
          icon={Minus}
          tone="muted"
          valueColor="text-zinc-500"
        />
        <StatCard
          value={negative?.toLocaleString()}
          label="부정 (negative)"
          icon={X}
          tone="danger"
          valueColor="text-red-500"
        />
      </div>

      {/* Aspect별 감성 분포 (전체 + aspect 드릴다운) */}
      <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
        <div className="grid grid-cols-1 gap-7 md:grid-cols-[minmax(240px,1fr)_1px_minmax(220px,0.85fr)]">
          {/* 좌: 주제 선택 목록 (전체 + aspect) */}
          <div className="flex flex-col gap-1">
            <div className="text-[15px] font-bold text-zinc-900">
              Aspect 감성 분포
            </div>
            <p className="mb-2 text-xs font-medium text-zinc-400">
              조항 수 기준 · 막대를 누르면 오른쪽에 감성 구성이 표시됩니다
            </p>
            {/* 전체 — 막대 없이 라벨/건수만 (전체 건수는 aspect 스케일 밖) */}
            <button
              type="button"
              onClick={() => setActiveAspect(null)}
              className={cn(
                "grid grid-cols-[84px_1fr_auto_16px] items-center gap-2.5 rounded-xl border-l-2 px-2 py-2 text-left transition-colors",
                isAll
                  ? "border-violet-500 bg-violet-50"
                  : "border-transparent hover:bg-zinc-50 hover:cursor-pointer",
              )}
            >
              <span
                className={cn(
                  "truncate text-right text-xs font-bold",
                  isAll ? "text-violet-700" : "text-zinc-600",
                )}
              >
                전체
              </span>
              <span aria-hidden />
              <span className="min-w-9 text-right text-xs font-bold tabular-nums text-zinc-800">
                {summary.total?.toLocaleString()}
              </span>
              <ChevronRight
                className={cn(
                  "h-3.5 w-3.5 transition-colors",
                  isAll ? "text-violet-600" : "text-zinc-300",
                )}
              />
            </button>
            <div className="my-1 h-px bg-zinc-100" />
            {/* aspect 목록 — 막대는 1위 aspect(maxAspect) 기준 스케일 */}
            {aspectData.map((a) => {
              const sel = !isAll && a.key === selectedKey;
              return (
                <button
                  key={a.key}
                  type="button"
                  onClick={() => setActiveAspect(a.key)}
                  className={cn(
                    "grid grid-cols-[84px_1fr_auto_16px] items-center gap-2.5 rounded-xl border-l-2 px-2 py-2 text-left transition-colors",
                    sel
                      ? "border-violet-500 bg-violet-50"
                      : "border-transparent hover:bg-zinc-50 hover:cursor-pointer",
                  )}
                >
                  <span
                    className={cn(
                      "truncate text-right text-xs font-semibold",
                      sel ? "text-violet-700" : "text-zinc-600",
                    )}
                  >
                    {a.name}
                  </span>
                  <span className="h-2.5 overflow-hidden rounded-full bg-zinc-100">
                    <span
                      className={cn(
                        "block h-full rounded-full bg-linear-to-r",
                        sel
                          ? "from-violet-600 to-violet-400"
                          : "from-blue-500 to-blue-400",
                      )}
                      style={{ width: `${(a.value / maxAspect) * 100}%` }}
                    />
                  </span>
                  <span className="min-w-9 text-right text-xs font-bold tabular-nums text-zinc-800">
                    {a.value.toLocaleString()}
                  </span>
                  <ChevronRight
                    className={cn(
                      "h-3.5 w-3.5 transition-colors",
                      sel ? "text-violet-600" : "text-zinc-300",
                    )}
                  />
                </button>
              );
            })}
          </div>

          {/* 구분선 */}
          <div className="hidden self-stretch bg-zinc-100 md:block" />

          {/* 우: 선택 주제 + 설명 + 도넛 */}
          <div className="flex flex-col">
            <div className="flex items-center gap-2">
              <span className="h-2 w-2 shrink-0 rounded-full bg-violet-600" />
              <span className="truncate font-extrabold text-violet-700">
                {selectedLabel}
              </span>
            </div>
            <div className="mt-1 text-xs font-medium text-zinc-400">
              {selectedDesc}
            </div>

            <div className="relative mx-auto mt-6 h-44 w-44">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={drillData}
                    cx="50%"
                    cy="50%"
                    innerRadius={56}
                    outerRadius={82}
                    paddingAngle={3}
                    nameKey="name"
                    dataKey="value"
                    stroke="none"
                  >
                    {drillData.map((d) => (
                      <Cell key={d.name} fill={SENTIMENT_COLORS[d.name]} />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="pointer-events-none absolute inset-0 grid place-items-center">
                <div className="text-center">
                  <div className="text-3xl font-extrabold leading-none tabular-nums text-zinc-900">
                    {drillTotal.toLocaleString()}
                  </div>
                  <div className="mt-1 text-[11px] font-semibold text-zinc-400">
                    총 문장
                  </div>
                </div>
              </div>
            </div>

            <div className="mt-6 flex flex-col gap-3">
              {drillData.map((d) => (
                <div key={d.name}>
                  <div className="flex items-center gap-2 text-[13px]">
                    <span
                      className="h-2.5 w-2.5 shrink-0 rounded-full"
                      style={{ background: SENTIMENT_COLORS[d.name] }}
                    />
                    <span className="font-semibold text-zinc-600">
                      {SENTIMENT_LABELS[d.name]}
                    </span>
                    <span className="ml-auto font-semibold tabular-nums text-zinc-400">
                      {d.value.toLocaleString()}건
                    </span>
                    <span className="min-w-12 text-right font-extrabold tabular-nums text-zinc-800">
                      {d.percent}%
                    </span>
                  </div>
                  <div className="mt-1.5 h-2 overflow-hidden rounded-full bg-zinc-100">
                    <div
                      className="h-full rounded-full"
                      style={{
                        width: `${d.percent}%`,
                        background: SENTIMENT_COLORS[d.name],
                      }}
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Table */}
      <DataTable
        columns={columns}
        items={items}
        rowKey={(item) => item.clauseId}
        title={`절 라벨링 결과 상세`}
        toolbar={
          <>
            <Select
              value={aspectFilter}
              onValueChange={(v) => {
                setAspectFilter(v);
                setPage(1);
              }}
            >
              <SelectTrigger className="h-7 w-40 text-xs">
                <SelectValue placeholder="Aspect" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">전체 Aspect</SelectItem>
                {aspectOptions.map((a) => (
                  <SelectItem key={a} value={a}>
                    {aspectLabelOf(taxonomy, a)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <FilterPills
              options={SENTIMENT_FILTER_OPTIONS}
              value={filter}
              onChange={(value) => {
                setFilter(value);
                setPage(1);
              }}
            />
          </>
        }
        page={page}
        totalPages={totalPages}
        totalCount={totalCount}
        onPageChange={setPage}
        loading={tableLoading}
      />
    </div>
  );
}
