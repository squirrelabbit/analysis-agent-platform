import { Check, FileText, Minus, X, type LucideIcon } from "lucide-react";
import type { GenuinenessBuild, GenuinenessItem } from "../../models/build";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { StatCard, type StatTone } from "@/components/common/cards/StatCard";
import { DonutChart, DistributionLegend } from "@/components/common/charts";
import {
  GENUINENESS_LABELS,
  GENUINENESS_COLORS,
  GENUINENESS_BADGE,
  GENUINENESS_ORDER,
  type Genuineness,
} from "@/features/versions/constants/genuineness";
import { Badge } from "@/components/ui/badge";
import { useBuildVersion } from "../../hooks/build.query";
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

// 필터 옵션: "전체" + 진성 3분류 (라벨은 GENUINENESS_LABELS 단일 출처).
const FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  ...GENUINENESS_ORDER.map((key) => ({
    label: GENUINENESS_LABELS[key],
    value: key,
  })),
];

export function GenuinenessBadge({ value }: { value: string }) {
  // min-w-14로 가장 긴 라벨(비진성/불확실, 3글자) 폭에 맞춰 동일 폭 + 가운데 정렬.
  // 진성(2글자)도 같은 폭이 되고, 더 긴 텍스트면 늘어나 clip 안 됨.
  return (
    <Badge className={cn("min-w-14", GENUINENESS_BADGE[value as Genuineness])}>
      {GENUINENESS_LABELS[value as Genuineness]}
    </Badge>
  );
}

const COLUMNS: Column<GenuinenessItem>[] = [
  {
    header: "문서 ID",
    headerClassName: "w-48",
    cell: (item) => <DocIdCell id={item.docId} />,
  },
  {
    header: "정제 텍스트",
    cell: (item) => <ExpandableTextCell text={item.cleanedText} />,
  },
  {
    header: "판별 결과",
    headerClassName: "w-36 text-center",
    cell: (item) => (
      <td className="px-4 py-3 text-center">
        <GenuinenessBadge value={item.genuineness} />
      </td>
    ),
  },
  {
    header: "사유",
    cell: (item) => (
      <td className="px-4 py-3 text-xs text-zinc-500 leading-relaxed max-w-sm">
        {item.reason}
      </td>
    ),
  },
];

export default function GenuinenessTab() {
  const [filter, setFilter] = useState<string>("");
  const [page, setPage] = useState(1);
  const pageSize = 10;

  // 서버 페이징 + 서버 필터: 표는 서버가 필터/페이징해 준 현재 페이지(items)만 렌더.
  const { data, isLoading, isPlaceholderData } = useBuildVersion(
    "doc_genuineness",
    undefined,
    {
      limit: pageSize,
      offset: (page - 1) * pageSize,
      genuineness: filter || undefined,
    },
  ) as {
    data: GenuinenessBuild | undefined;
    isLoading: boolean;
    isPlaceholderData: boolean;
  };

  // 페이지/필터 변경으로 새 데이터 도착 전(이전 데이터 표시 중) → 표 로딩 오버레이.
  const tableLoading = isPlaceholderData;
  const {
    summary,
    applied,
    items,
    pagination,
    status,
    progress,
    durationSeconds,
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
      <BuildTabEmpty type="doc_genuineness" status={status} />
    );
  }

  const { genuineness, total } = summary;

  // 판별 결과 요약 카드 (전체 + 진성 3분류).
  const stats: {
    value: number;
    label: string;
    icon: LucideIcon;
    tone: StatTone;
    valueColor?: string;
  }[] = [
    { value: total, label: "전체 문서", icon: FileText, tone: "neutral" },
    {
      value: genuineness.genuine_review,
      label: "진성",
      icon: Check,
      tone: "ok",
      valueColor: "text-emerald-600",
    },
    {
      value: genuineness.non_review,
      label: "비진성",
      icon: X,
      tone: "danger",
      valueColor: "text-red-500",
    },
    {
      value: genuineness.uncertain,
      label: "불확실",
      icon: Minus,
      tone: "muted",
      valueColor: "text-zinc-400",
    },
  ];

  const pct = (value: number) =>
    total > 0 ? ((value / total) * 100).toFixed(1) : "0.0";

  // 도넛/범례 공용 분포 데이터 (진성/비진성/불확실 3분류, mixed 제거).
  // summary.genuineness 키가 snake_case enum 값이라 상수·카운트를 같은 key로 바로 인덱싱.
  const ratioData = GENUINENESS_ORDER.map((key) => ({
    key,
    label: GENUINENESS_LABELS[key],
    value: genuineness[key],
    color: GENUINENESS_COLORS[key],
    percent: total > 0 ? Math.round((genuineness[key] / total) * 1000) / 10 : 0,
  })).filter((d) => d.value > 0);

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  return (
    <div className="space-y-5">
      {/* 메타 */}
      <BuildMetaBar
        status={status}
        durationSeconds={durationSeconds}
        applied={applied}
      />

      <BuildRunningBanner status={status} progress={progress} hasPrevious />

      {/* 판별 결과 요약 */}
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

      {/* 판별 결과 분포 */}
      <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
        <div className="text-[15px] font-bold text-zinc-900">
          판별 결과 분포
        </div>
        <div className="mt-1 text-xs font-medium text-zinc-400">
          전체 {total?.toLocaleString()}건 기준 · 진성 비율{" "}
          {pct(genuineness.genuine_review)}%
        </div>
        <div className="mt-5 flex flex-wrap items-center gap-7">
          <DonutChart
            data={ratioData}
            size={132}
            innerRadius={42}
            outerRadius={62}
            paddingAngle={2}
          />
          <DistributionLegend items={ratioData} className="min-w-60 flex-1" />
        </div>
      </div>

      {/* Table */}
      <DataTable
        columns={COLUMNS}
        items={items}
        rowKey={(item) => item.docId}
        title={`판별 결과 상세`}
        toolbar={
          <FilterPills
            options={FILTER_OPTIONS}
            value={filter}
            onChange={(value) => {
              setFilter(value);
              setPage(1);
            }}
          />
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
