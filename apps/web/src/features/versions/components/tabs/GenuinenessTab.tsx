import { MetricCard } from "@/components/common/cards/MetricCard";
import { Pie, PieChart } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
import type { GenuinenessBuild } from "../../models/build";
import { useState } from "react";
import { Card, CardContent, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useBuildVersion } from "../../hooks/build.query";

const COLORS = {
  genuineReview: "#10b981", // emerald-500
  mixed: "#f59e0b", // amber-500
  nonReview: "#f87171", // red-400
  uncertain: "#a1a1aa", // zinc-400
};

const chartConfig = {
  genuineReview: { label: "진성" },
  mixed: { label: "혼합" },
  nonReview: { label: "비진성" },
  uncertain: { label: "불확실" },
} as const;

const FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  { label: "진성", value: "genuine_review" },
  { label: "비진성", value: "non_review" },
  { label: "불확실", value: "uncertain" },
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
  const { summary, applied, items, pagination } = data || {};

  if (!summary) {
    return (
      <p className="text-sm text-zinc-500">
        표시할 진위성 분석 요약이 없습니다.
      </p>
    );
  }

  const { genuineness, total } = summary;
  const { genuineReview, mixed, nonReview, uncertain } = genuineness;

  const pieData = [
    {
      type: "genuineReview",
      value: genuineReview,
      fill: COLORS.genuineReview,
    },

    {
      type: "mixed",
      value: mixed,
      fill: COLORS.mixed,
    },

    {
      type: "nonReview",
      value: nonReview,
      fill: COLORS.nonReview,
    },

    {
      type: "uncertain",
      value: uncertain,
      fill: COLORS.uncertain,
    },
  ];

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  return (
    <div className="space-y-5">
      {/* Metrics */}
      <div>
        <p className="text-xs font-medium text-zinc-400 uppercase tracking-widest mb-2">
          분석 정보
        </p>
        <div className="grid grid-cols-2 gap-2">
          <MetricCard
            label="프롬프트 버전"
            value={applied?.promptVersion ?? "-"}
          />
          <MetricCard label="분석 모델" value={items?.[0]?.source ?? "-"} />
        </div>
      </div>
      {/* Charts + info */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <Card>
          <CardContent>
            <CardTitle className="text-xs text-zinc-500">판별</CardTitle>

            <div className="grid grid-cols-2 gap-2">
              <MetricCard label="전체 문서" value={total} />
              <MetricCard
                label="진성"
                value={genuineReview}
                valueColor="text-emerald-600"
              />
              <MetricCard
                label="비진성"
                value={nonReview}
                valueColor="text-red-400"
              />
              <MetricCard
                label="불확실"
                value={uncertain}
                valueColor="text-zinc-400"
              />
            </div>
          </CardContent>
        </Card>
        {/* Donut chart */}
        <Card>
          <CardContent>
            <CardTitle className="text-xs text-zinc-500">분포 비율</CardTitle>

            <ChartContainer
              config={chartConfig}
              className="mx-auto aspect-square max-h-62.5"
            >
              <PieChart>
                <ChartTooltip
                  cursor={false}
                  content={<ChartTooltipContent hideLabel />}
                />
                <Pie
                  data={pieData}
                  dataKey="value"
                  nameKey="type"
                  innerRadius={60}
                />
              </PieChart>
            </ChartContainer>
          </CardContent>
        </Card>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-zinc-100 bg-white overflow-hidden">
        <div className="px-4 py-3 border-b border-zinc-50 flex items-center justify-between flex-wrap gap-2">
          <span className="text-xs font-medium text-zinc-500">
            판별 결과 상세 ({totalCount}건)
          </span>
          <div className="flex gap-1.5 flex-wrap">
            {FILTER_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                onClick={() => {
                  setFilter(opt.value);
                  setPage(1);
                }}
                className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
                  filter === opt.value
                    ? "bg-zinc-800 text-white border-zinc-800"
                    : "bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50"
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-50">
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide w-48">
                  문서 ID
                </th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide">
                  정제 텍스트
                </th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide w-36">
                  판별 결과
                </th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide">
                  사유
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-50">
              {!items || items.length === 0 ? (
                <tr>
                  <td
                    colSpan={4}
                    className="text-center py-8 text-sm text-zinc-400"
                  >
                    해당 항목이 없습니다
                  </td>
                </tr>
              ) : (
                items.map((item) => (
                  <tr
                    key={item.docId}
                    className="hover:bg-zinc-50/60 transition-colors"
                  >
                    <td className="px-4 py-3 font-mono text-xs text-zinc-400 max-w-45 truncate">
                      {item.docId}
                    </td>
                    <td className="px-4 py-3 text-xs text-zinc-500 leading-relaxed max-w-sm">
                      {item.cleanedText}
                    </td>
                    <td className="px-4 py-3">
                      {/* <p>{item.genuineness}</p> */}
                      <GenuinenessBadge value={item.genuineness} />
                    </td>
                    <td className="px-4 py-3 text-xs text-zinc-500 leading-relaxed max-w-sm">
                      {item.reason}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
          <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-100">
            <p className="text-xs text-zinc-400">총 {totalCount}개</p>

            <div className="flex items-center gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={page === 1}
                onClick={() => setPage((p) => p - 1)}
              >
                이전
              </Button>

              <span className="text-xs text-zinc-500">
                {page} / {totalPages}
              </span>

              <Button
                size="sm"
                variant="outline"
                disabled={page === totalPages}
                onClick={() => setPage((p) => p + 1)}
              >
                다음
              </Button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
