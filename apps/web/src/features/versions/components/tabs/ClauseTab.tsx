import { useState } from "react";
import { FileText, Check, Minus, X, type LucideIcon } from "lucide-react";
import { StatCard, type StatTone } from "@/components/common/cards/StatCard";
import {
  DonutChart,
  DistributionLegend,
  SelectableBarRow,
} from "@/components/common/charts";
import {
  SENTIMENT_COLORS,
  SENTIMENT_LABELS,
  SENTIMENT_ORDER,
  SENTIMENT_BADGE,
  type Sentiment,
  SENTIMENT_FILTER_OPTIONS,
} from "@/features/versions/constants/sentiment";
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
  BuildMetaBar,
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  isBuildRunning,
} from "../BuildStatusMeta";

// 드릴다운 selector의 "전체" 항목 sentinel key (실제 aspect key와 충돌 방지).
const ALL_KEY = "__all__";

function SentimentBadge({ value }: { value: string }) {
  return (
    <Badge className={SENTIMENT_BADGE[value as Sentiment]}>
      {SENTIMENT_LABELS[value as Sentiment]}
    </Badge>
  );
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

  const stats: {
    value: number;
    label: string;
    icon: LucideIcon;
    tone: StatTone;
    valueColor?: string;
  }[] = [
    {
      value: summary.total,
      label: "총 문장 수",
      icon: FileText,
      tone: "neutral",
    },
    {
      value: positive,
      label: "긍정 (positive)",
      icon: Check,
      tone: "ok",
      valueColor: "text-emerald-600",
    },
    {
      value: neutral,
      label: "중립 (neutral)",
      icon: Minus,
      tone: "muted",
      valueColor: "text-zinc-500",
    },
    {
      value: negative,
      label: "부정 (negative)",
      icon: X,
      tone: "danger",
      valueColor: "text-red-500",
    },
  ];

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
  // key/label/color를 함께 담아 도넛(DonutChart)·범례(DistributionLegend)가 그대로 소비.
  const drillData = SENTIMENT_ORDER.map((name) => {
    const base = {
      key: name,
      label: SENTIMENT_LABELS[name],
      color: SENTIMENT_COLORS[name],
    };
    if (isAll) {
      const value = overallByName[name] ?? 0;
      const percent =
        summary.total > 0 ? Math.round((value / summary.total) * 1000) / 10 : 0;
      return { ...base, value, percent };
    }
    const s = selectedAspect.sentiment[name];
    return { ...base, value: s?.count ?? 0, percent: s?.percent ?? 0 };
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
      <BuildMetaBar
        status={status}
        durationSeconds={durationSeconds}
        applied={applied}
      />

      <BuildRunningBanner status={status} progress={progress} hasPrevious />

      {/* 분류 현황 */}
      <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
        {stats.map((s) => (
          <StatCard
            key={s.label}
            value={s.value?.toLocaleString()}
            label={s.label}
            icon={s.icon}
            tone={s.tone}
            valueColor={s.valueColor}
          />
        ))}
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
            <SelectableBarRow
              label="전체"
              count={summary.total?.toLocaleString()}
              selected={isAll}
              onClick={() => setActiveAspect(null)}
              showBar={false}
              labelClassName="font-bold"
            />
            <div className="my-1 h-px bg-zinc-100" />
            {/* aspect 목록 — 막대는 1위 aspect(maxAspect) 기준 스케일 */}
            {aspectData.map((a) => (
              <SelectableBarRow
                key={a.key}
                label={a.name}
                count={a.value.toLocaleString()}
                value={a.value}
                max={maxAspect}
                selected={!isAll && a.key === selectedKey}
                onClick={() => setActiveAspect(a.key)}
              />
            ))}
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

            <DonutChart
              data={drillData}
              size={176}
              innerRadius={56}
              outerRadius={82}
              paddingAngle={3}
              className="mx-auto mt-6"
              center={
                <div className="text-center">
                  <div className="text-3xl font-extrabold leading-none tabular-nums text-zinc-900">
                    {drillTotal.toLocaleString()}
                  </div>
                  <div className="mt-1 text-[11px] font-semibold text-zinc-400">
                    총 문장
                  </div>
                </div>
              }
            />

            <DistributionLegend items={drillData} className="mt-6 gap-3" />
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
