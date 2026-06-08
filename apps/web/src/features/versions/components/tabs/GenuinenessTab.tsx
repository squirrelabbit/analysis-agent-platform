import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { AlignLeft, Box, Check, FileText, Minus, X } from "lucide-react";
import type { GenuinenessBuild, GenuinenessItem } from "../../models/build";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { StatCard } from "@/components/common/cards/StatCard";
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
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  BuildTimerChip,
  isBuildRunning,
} from "../BuildStatusMeta";

const COLORS = {
  genuineReview: "#10b981", // emerald-500
  nonReview: "#f87171", // red-400
  uncertain: "#a1a1aa", // zinc-400
};

// 값은 백엔드 genuineness 컬럼 원본(snake_case)과 일치해야 서버 필터가 동작.
// mixed는 planner가 더 이상 생성하지 않아(backward-compat enum만 존재) UI에서 제거.
const FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  { label: "진성", value: "genuine_review" },
  { label: "비진성", value: "non_review" },
  { label: "불확실", value: "uncertain" },
];

export function GenuinenessBadge({ value }: { value: string }) {
  const map: Record<string, string> = {
    genuine_review: "bg-emerald-50 text-emerald-800 border-emerald-200",
    non_review: "bg-red-50 text-red-800 border-red-200",
    uncertain: "bg-zinc-100 text-zinc-400 border-zinc-200",
  };
  const labels: Record<string, string> = {
    genuine_review: "진성",
    non_review: "비진성",
    uncertain: "불확실",
  };
  // min-w-14로 가장 긴 라벨(비진성/불확실, 3글자) 폭에 맞춰 동일 폭 + 가운데 정렬.
  // 진성(2글자)도 같은 폭이 되고, 더 긴 텍스트면 늘어나 clip 안 됨.
  return <Badge className={cn("min-w-14", map[value])}>{labels[value]}</Badge>;
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
  const { genuineReview, nonReview, uncertain } = genuineness;

  const pct = (value: number) =>
    total > 0 ? ((value / total) * 100).toFixed(1) : "0.0";

  // 도넛/범례 공용 분포 데이터 (진성/비진성/불확실 3분류, mixed 제거).
  const ratioData = [
    { key: "진성", value: genuineReview, color: COLORS.genuineReview },
    { key: "비진성", value: nonReview, color: COLORS.nonReview },
    { key: "불확실", value: uncertain, color: COLORS.uncertain },
  ].filter((d) => d.value > 0);

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  return (
    <div className="space-y-5">
      {/* 메타 */}
      <div className="flex flex-wrap items-center gap-3 text-xs text-zinc-500">
        <BuildTimerChip status={status} durationSeconds={durationSeconds} />
        <span className="h-3 w-px bg-zinc-200" />
        <span className="inline-flex items-center gap-1.5 font-medium">
          <AlignLeft className="h-3.5 w-3.5 text-zinc-400" strokeWidth={1.8} />
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

      {/* 판별 결과 요약 */}
      <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
        <StatCard
          value={total?.toLocaleString()}
          label="전체 문서"
          icon={FileText}
          tone="neutral"
        />
        <StatCard
          value={genuineReview?.toLocaleString()}
          label="진성"
          icon={Check}
          tone="ok"
          valueColor="text-emerald-600"
        />
        <StatCard
          value={nonReview?.toLocaleString()}
          label="비진성"
          icon={X}
          tone="danger"
          valueColor="text-red-500"
        />
        <StatCard
          value={uncertain?.toLocaleString()}
          label="불확실"
          icon={Minus}
          tone="muted"
          valueColor="text-zinc-400"
        />
      </div>

      {/* 판별 결과 분포 */}
      <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
        <div className="text-[15px] font-bold text-zinc-900">
          판별 결과 분포
        </div>
        <div className="mt-1 text-xs font-medium text-zinc-400">
          전체 {total?.toLocaleString()}건 기준 · 진성 비율 {pct(genuineReview)}
          %
        </div>
        <div className="mt-5 flex flex-wrap items-center gap-7">
          <ResponsiveContainer width={132} height={132} className="shrink-0">
            <PieChart>
              <Pie
                data={ratioData}
                dataKey="value"
                nameKey="key"
                cx="50%"
                cy="50%"
                innerRadius={42}
                outerRadius={62}
                paddingAngle={2}
                stroke="none"
              >
                {ratioData.map((d) => (
                  <Cell key={d.key} fill={d.color} />
                ))}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
          <div className="flex min-w-60 flex-1 flex-col gap-4">
            {ratioData.map((d) => (
              <div key={d.key}>
                <div className="flex items-center gap-2 text-[13.5px]">
                  <span
                    className="h-2.5 w-2.5 shrink-0 rounded-full"
                    style={{ background: d.color }}
                  />
                  <span className="font-semibold text-zinc-600">{d.key}</span>
                  <span className="ml-auto font-semibold tabular-nums text-zinc-400">
                    {d.value.toLocaleString()}건
                  </span>
                  <span className="min-w-14 text-right font-extrabold tabular-nums text-zinc-800">
                    {pct(d.value)}%
                  </span>
                </div>
                <div className="mt-2 h-2 overflow-hidden rounded-full bg-zinc-100">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${pct(d.value)}%`,
                      background: d.color,
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
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
