import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import { MetricCard } from "@/components/common/cards/MetricCard";
import type { ClauseBuild } from "../../models/build";
import { Badge } from "@/components/ui/badge";
import { useBuildVersion } from "../../hooks/build.query";

const SENTIMENT_COLORS: Record<string, string> = {
  positive: "#10b981",
  neutral: "#a1a1aa",
  negative: "#ef4444",
};

const SENTIMENT_FILTER_OPTIONS: { label: string; value: string | "" }[] = [
  { label: "전체", value: "" },
  { label: "긍정", value: "positive" },
  { label: "중립", value: "neutral" },
  { label: "부정", value: "negative" },
];

function SentimentBadge({ value }: { value: string }) {
  const map: Record<string, string> = {
    positive: "bg-emerald-50 text-emerald-800 border-emerald-200",
    neutral: "bg-zinc-100 text-zinc-600 border-zinc-200",
    negative: "bg-red-50 text-red-800 border-red-200",
  };
  const labels: Record<string, string> = {
    positive: "긍정",
    neutral: "중립",
    negative: "부정",
  };
  return <Badge className={map[value]}>{labels[value]}</Badge>;
}

export function ClauseTab() {
  const { data } = useBuildVersion("clause_label") as { data: ClauseBuild | undefined };
  const { summary, items, applied, durationSeconds } = data || {};
  const [filter, setFilter] = useState<string | "">("");
  const [page, setPage] = useState(1);
  const pageSize = 10;

  if (!summary) {
    return <p className="text-sm text-zinc-500">표시할 분류 요약이 없습니다.</p>;
  }

  const {
    sentiment: { positive, neutral, negative },
  } = summary;

  const aspectData = Object.entries(summary.aspect)
    .sort(([, a], [, b]) => b - a)
    .map(([name, value]) => ({ name, value }));

  // Sentiment pie data
  const sentimentData = [
    { name: "positive", value: positive, fill: SENTIMENT_COLORS.positive },
    { name: "neutral", value: neutral, fill: SENTIMENT_COLORS.neutral },
    { name: "negative", value: negative, fill: SENTIMENT_COLORS.negative },
  ];

  const filtered = filter ? items?.filter((i) => i.sentiment === filter) : items;
  const paginatedItems = filtered?.slice((page - 1) * pageSize, page * pageSize);
  const totalPages = Math.ceil((filtered?.length ?? 0) / pageSize);

  return (
    <div className="space-y-5">
      {/* Metrics */}
      <div>
        <p className="text-xs font-medium text-zinc-400 uppercase tracking-widest mb-2">
          분류 현황
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          <MetricCard label="총 조항 수" value={summary.total} />
          <MetricCard
            label="긍정 (positive)"
            value={positive}
            valueColor="text-emerald-600"
          />
          <MetricCard
            label="중립 (neutral)"
            value={neutral}
            valueColor="text-zinc-500"
          />
          <MetricCard
            label="부정 (negative)"
            value={negative}
            valueColor="text-red-500"
          />
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {/* Aspect bar chart */}
        <div className="rounded-xl border border-zinc-100 bg-white p-4">
          <p className="text-xs font-medium text-zinc-500 uppercase tracking-wider mb-3">
            Aspect 분포
          </p>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart
              data={aspectData}
              layout="vertical"
              margin={{ top: 0, right: 16, bottom: 0, left: 10 }}
            >
              <XAxis
                type="number"
                tick={{ fontSize: 11, fill: "#a1a1aa" }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                type="category"
                dataKey="name"
                tick={{ fontSize: 11, fill: "#71717a" }}
                axisLine={false}
                tickLine={false}
                width={88}
              />
              {/* <Tooltip
                formatter={(v: number) => [`${v}건`, "조항 수"]}
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
              /> */}
              <Bar
                dataKey="value"
                fill="#3b82f6"
                radius={[0, 3, 3, 0]}
                barSize={10}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Sentiment donut + meta */}
        <div className="space-y-3">
          <div className="rounded-xl border border-zinc-100 bg-white p-4">
            <p className="text-xs font-medium text-zinc-500 uppercase tracking-wider mb-2">
              감성 분포
            </p>
            <div className="flex items-center gap-4">
              <ResponsiveContainer width={120} height={120}>
                <PieChart>
                  <Pie
                    data={sentimentData}
                    cx="50%"
                    cy="50%"
                    innerRadius={34}
                    outerRadius={54}
                    paddingAngle={3}
                    nameKey="type"
                    dataKey="value"
                  >
                    {sentimentData.map((entry) => (
                      <Cell
                        key={entry.name}
                        fill={SENTIMENT_COLORS[entry.name]}
                      />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="flex flex-col gap-2">
                {sentimentData.map((d) => (
                  <div
                    key={d.name}
                    className="flex items-center gap-2 text-xs text-zinc-500"
                  >
                    <span
                      className="w-2.5 h-2.5 rounded-sm inline-block"
                      style={{ background: SENTIMENT_COLORS[d.name] }}
                    />
                    <span>{d.name}</span>
                    <span className="font-semibold text-zinc-700 ml-auto pl-2">
                      {Math.round((d.value / summary.total) * 100)}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Meta */}
          <div className="rounded-xl border border-zinc-100 bg-white px-4 py-3 space-y-2">
            {[
              {
                label: "프롬프트 버전",
                value: (
                  <span className="font-mono text-xs">
                    {applied?.promptVersion}
                  </span>
                ),
              },
              {
                label: "소요 시간",
                value:
                  (durationSeconds ?? 0) > 0
                    ? `${Math.floor((durationSeconds ?? 0) / 60)}분 ${(durationSeconds ?? 0) % 60}초`
                    : `${durationSeconds ?? 0}초`,
              },
            ].map(({ label, value }) => (
              <div
                key={label}
                className="flex items-center justify-between text-xs border-b border-zinc-50 pb-2 last:border-0 last:pb-0"
              >
                <span className="text-zinc-400">{label}</span>
                <span className="font-medium text-zinc-700">{value}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-zinc-100 bg-white overflow-hidden">
        <div className="px-4 py-3 border-b border-zinc-50 flex items-center justify-between flex-wrap gap-2">
          <span className="text-xs font-medium text-zinc-500">
            조항 결과 상세 ({items?.length ?? 0}건)
          </span>
          <div className="flex gap-1.5 flex-wrap">
            {SENTIMENT_FILTER_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                onClick={() => setFilter(opt.value)}
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
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide w-80">
                  조항
                </th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide w-28">
                  Aspect
                </th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide w-24">
                  감성
                </th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide">
                  조항 텍스트
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-50">
              {filtered?.length === 0 ? (
                <tr>
                  <td
                    colSpan={4}
                    className="text-center py-8 text-sm text-zinc-400"
                  >
                    해당 항목이 없습니다
                  </td>
                </tr>
              ) : (
                paginatedItems?.map((item) => (
                  <tr
                    key={item.clauseId}
                    className="hover:bg-zinc-50/60 transition-colors"
                  >
                    <td className="px-4 py-3 font-mono text-xs text-zinc-400 max-w-40 truncate">
                      {item.clause}
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs text-zinc-500">
                        {item.aspect}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <SentimentBadge value={item.sentiment} />
                    </td>
                    <td className="px-4 py-3 text-xs text-zinc-500 leading-relaxed max-w-sm">
                      {item.clause}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
          <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-100">
            <p className="text-xs text-zinc-400">총 {filtered?.length ?? 0}개</p>

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
