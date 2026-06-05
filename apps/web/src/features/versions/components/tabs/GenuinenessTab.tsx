import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { AlignLeft, Box, Check, Clock, FileText, Minus, X } from "lucide-react";
import type { GenuinenessBuild, GenuinenessItem } from "../../models/build";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { StatCard } from "@/components/common/cards/StatCard";
import { Badge } from "@/components/ui/badge";
import { useBuildVersion } from "../../hooks/build.query";
import {
  DataTable,
  ExpandableTextCell,
  FilterPills,
  type Column,
} from "../DataTable";
import { formatSecond } from "@/shared/utils/format";

const COLORS = {
  genuineReview: "#10b981", // emerald-500
  mixed: "#f59e0b", // amber-500
  nonReview: "#f87171", // red-400
  uncertain: "#a1a1aa", // zinc-400
};

// 값은 백엔드 genuineness 컬럼 원본(snake_case)과 일치해야 서버 필터가 동작.
const FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  { label: "진성", value: "genuine_review" },
  { label: "비진성", value: "non_review" },
  { label: "불확실", value: "uncertain" },
  { label: "혼합", value: "mixed" },
];

export function GenuinenessBadge({ value }: { value: string }) {
  const map: Record<string, string> = {
    genuine_review: "bg-emerald-50 text-emerald-800 border-emerald-200",
    mixed: "bg-amber-50 text-amber-800 border-amber-200",
    non_review: "bg-zinc-100 text-zinc-600 border-zinc-200",
    uncertain: "bg-zinc-100 text-zinc-400 border-zinc-200",
  };
  const labels: Record<string, string> = {
    genuine_review: "진성",
    mixed: "혼합",
    non_review: "비진성",
    uncertain: "불확실",
  };
  return <Badge className={cn(map[value])}>{labels[value]}</Badge>;
}

const COLUMNS: Column<GenuinenessItem>[] = [
  {
    header: "문서 ID",
    headerClassName: "w-48",
    cell: (item) => (
      <td className="px-4 py-3 font-mono text-xs text-zinc-400 max-w-45 truncate">
        {item.docId}
      </td>
    ),
  },
  {
    header: "정제 텍스트",
    cell: (item) => <ExpandableTextCell text={item.cleanedText} />,
  },
  {
    header: "판별 결과",
    headerClassName: "w-36",
    cell: (item) => (
      <td className="px-4 py-3">
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
  const { data } = useBuildVersion("doc_genuineness", undefined, {
    limit: pageSize,
    offset: (page - 1) * pageSize,
    genuineness: filter || undefined,
  }) as {
    data: GenuinenessBuild | undefined;
  };
  const { summary, applied, items, pagination, durationSeconds } = data || {};

  if (!summary) {
    return (
      <p className="text-sm text-zinc-500">
        표시할 진위성 분석 요약이 없습니다.
      </p>
    );
  }

  const { genuineness, total } = summary;
  const { genuineReview, mixed, nonReview, uncertain } = genuineness;

  const pct = (value: number) =>
    total > 0 ? ((value / total) * 100).toFixed(1) : "0.0";

  // 도넛/범례 공용 분포 데이터. 혼합(mixed)은 0이면 숨겨 시안의 3분류 구성에 맞춘다.
  const ratioData = [
    { key: "진성", value: genuineReview, color: COLORS.genuineReview },
    { key: "비진성", value: nonReview, color: COLORS.nonReview },
    { key: "불확실", value: uncertain, color: COLORS.uncertain },
    { key: "혼합", value: mixed, color: COLORS.mixed },
  ].filter((d) => d.value > 0);

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  return (
    <div className="space-y-5">
      {/* 메타 */}
      <div className="flex flex-wrap items-center gap-3 text-xs text-zinc-500">
        <span className="inline-flex items-center gap-1.5 font-medium">
          <Clock className="h-3.5 w-3.5 text-zinc-400" strokeWidth={1.8} />
          소요 시간
          <b className="font-bold text-zinc-800">
            {formatSecond(durationSeconds)}
          </b>
        </span>
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
          <b className="font-bold text-zinc-800">{items?.[0]?.source ?? "-"}</b>
        </span>
      </div>

      {/* 판별 결과 요약 */}
      <div>
        <p className="mb-3 text-[13px] font-bold text-zinc-600">
          판별 결과 요약
        </p>
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
      </div>

      {/* 판별 결과 분포 */}
      <div>
        <p className="mb-3 text-[13px] font-bold text-zinc-600">
          판별 결과 분포
        </p>
        <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
          <div className="text-[15px] font-bold text-zinc-900">
            판별 결과 분포
          </div>
          <div className="mt-1 text-xs font-medium text-zinc-400">
            전체 {total?.toLocaleString()}건 기준 · 진성 비율{" "}
            {pct(genuineReview)}%
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
      </div>

      {/* Table */}
      <DataTable
        columns={COLUMNS}
        items={items}
        rowKey={(item) => item.docId}
        title={`판별 결과 상세 (${totalCount}건)`}
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
      />
    </div>
  );
}
