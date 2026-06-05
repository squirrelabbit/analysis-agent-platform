import { useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  LabelList,
} from "recharts";
import { MetricCard } from "@/components/common/cards/MetricCard";
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
  ExpandableTextCell,
  FilterPills,
  type Column,
} from "../DataTable";

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
  const [filter, setFilter] = useState<string | "">("");
  const [aspectFilter, setAspectFilter] = useState<string>("all");
  const [page, setPage] = useState(1);
  const pageSize = 10;

  // 서버 페이징 + 서버 필터: 표는 서버가 필터/페이징해 준 현재 페이지(items)만 렌더.
  const { data } = useBuildVersion("clause_label", undefined, {
    limit: pageSize,
    offset: (page - 1) * pageSize,
    aspect: aspectFilter === "all" ? undefined : aspectFilter,
    sentiment: filter || undefined,
  }) as { data: ClauseBuild | undefined };
  // taxonomy 조회 실패해도 aspectLabelOf가 key로 fallback하므로 화면은 동작한다.
  const { data: taxonomy } = useTaxonomy();
  const { summary, items, applied, durationSeconds, pagination } = data || {};

  if (!summary) {
    return (
      <p className="text-sm text-zinc-500">표시할 분류 요약이 없습니다.</p>
    );
  }

  const {
    sentiment: { positive, neutral, negative },
  } = summary;

  // summary.aspect는 snake_case key → 한글 label로 변환해 차트 축에 표시.
  const aspectData = Object.entries(summary.aspect)
    .sort(([, a], [, b]) => b - a)
    .map(([key, value]) => ({ name: aspectLabelOf(taxonomy, key), value }));

  // Sentiment pie data
  const sentimentData = [
    { name: "positive", value: positive, fill: SENTIMENT_COLORS.positive },
    { name: "neutral", value: neutral, fill: SENTIMENT_COLORS.neutral },
    { name: "negative", value: negative, fill: SENTIMENT_COLORS.negative },
  ];

  // aspect 옵션은 전체 분포(summary.aspect) 기준 — 현재 페이지 items가 아니라.
  const aspectOptions = Object.keys(summary.aspect);

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산의 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const loadedStart = totalCount === 0 ? 0 : (pagination?.offset ?? 0) + 1;
  const loadedEnd = (pagination?.offset ?? 0) + (items?.length ?? 0);

  const totalSec = Math.round(durationSeconds ?? 0);
  const durationLabel =
    totalSec >= 60
      ? `${Math.floor(totalSec / 60)}분 ${totalSec % 60}초`
      : `${totalSec}초`;

  const columns: Column<ClauseItem>[] = [
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
      header: "조항",
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
    {
      header: "조항 텍스트",
      cell: (item) => <ExpandableTextCell text={item.clause} />,
    },
  ];

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
              margin={{ top: 0, right: 40, bottom: 0, left: 10 }}
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
                interval={0}
                tick={{ fontSize: 11, fill: "#71717a" }}
                axisLine={false}
                tickLine={false}
                width={104}
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
              >
                <LabelList
                  dataKey="value"
                  position="right"
                  fontSize={11}
                  fill="#71717a"
                />
              </Bar>
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
                value: durationLabel,
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
      <DataTable
        columns={columns}
        items={items}
        rowKey={(item) => item.clauseId}
        title={`총 ${totalCount}건 중 ${loadedStart}–${loadedEnd}건 표시`}
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
      />
    </div>
  );
}
