import { useState } from "react";
import { FileText, Check, Minus, X, Clock, Box } from "lucide-react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
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
  ExpandableTextCell,
  FilterPills,
  type Column,
} from "../DataTable";
import { formatSecond } from "@/shared/utils/format";

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

  const maxAspect = Math.max(...aspectData.map((a) => a.value), 1);

  // 감성 도넛 데이터
  const sentimentData = [
    { name: "positive", value: positive },
    { name: "neutral", value: neutral },
    { name: "negative", value: negative },
  ];

  // aspect 옵션은 전체 분포(summary.aspect) 기준 — 현재 페이지 items가 아니라.
  const aspectOptions = Object.keys(summary.aspect);

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산의 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const loadedStart = totalCount === 0 ? 0 : (pagination?.offset ?? 0) + 1;
  const loadedEnd = (pagination?.offset ?? 0) + (items?.length ?? 0);

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

      {/* 분류 현황 */}
      <div>
        <p className="mb-3 text-[13px] font-bold text-zinc-600">분류 현황</p>
        <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
          <StatCard
            value={summary.total?.toLocaleString()}
            label="총 조항 수"
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
      </div>

      {/* 분포 */}
      <div>
        <p className="mb-3 text-[13px] font-bold text-zinc-600">분포</p>
        <div className="grid grid-cols-1 gap-3.5 md:grid-cols-3">
          {/* Aspect 건수 막대 */}
          <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm md:col-span-2">
            <div className="text-[15px] font-bold text-zinc-900">
              Aspect 분포
            </div>
            <div className="mt-1 text-xs font-medium text-zinc-400">
              언급된 조항 수 기준
            </div>
            <div className="mt-5 flex flex-col gap-3">
              {aspectData.map((a) => (
                <div
                  key={a.name}
                  className="grid grid-cols-[96px_1fr_auto] items-center gap-3"
                >
                  <span className="truncate text-right text-xs font-semibold text-zinc-500">
                    {a.name}
                  </span>
                  <div className="h-2.5 overflow-hidden rounded-full bg-zinc-100">
                    <div
                      className="h-full rounded-full bg-linear-to-r from-blue-500 to-blue-400"
                      style={{ width: `${(a.value / maxAspect) * 100}%` }}
                    />
                  </div>
                  <span className="min-w-9 text-right text-xs font-bold tabular-nums text-zinc-800">
                    {a.value.toLocaleString()}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* 감성 도넛 */}
          <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
            <div className="text-[15px] font-bold text-zinc-900">감성 분포</div>
            <div className="mt-1 text-xs font-medium text-zinc-400">
              전체 {summary.total?.toLocaleString()}건 기준
            </div>
            <div className="mt-4 flex flex-col items-center gap-6">
              <ResponsiveContainer
                width={120}
                height={120}
                className="shrink-0"
              >
                <PieChart>
                  <Pie
                    data={sentimentData}
                    cx="50%"
                    cy="50%"
                    innerRadius={34}
                    outerRadius={54}
                    paddingAngle={3}
                    nameKey="name"
                    dataKey="value"
                    stroke="none"
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
              <div className="flex flex-1 flex-col gap-2.5">
                {sentimentData.map((d) => (
                  <div
                    key={d.name}
                    className="flex items-center gap-2 text-[13px]"
                  >
                    <span
                      className="h-2.5 w-2.5 shrink-0 rounded-full"
                      style={{ background: SENTIMENT_COLORS[d.name] }}
                    />
                    <span className="font-semibold text-zinc-600">
                      {d.name}
                    </span>
                    <span className="ml-auto font-extrabold tabular-nums text-zinc-800">
                      {summary.total > 0
                        ? Math.round((d.value / summary.total) * 100)
                        : 0}
                      %
                    </span>
                  </div>
                ))}
              </div>
            </div>
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
