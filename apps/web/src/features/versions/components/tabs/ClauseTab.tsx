import { useState, type ReactNode } from "react";
import {
  FileText,
  Check,
  Loader2,
  Minus,
  Pencil,
  RotateCcw,
  X,
  type LucideIcon,
} from "lucide-react";
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
} from "@/features/versions/constants/sentiment";
import type { ClauseBuild, ClauseItem, ClauseModelResult } from "../../models/build";
import { Badge } from "@/components/ui/badge";
import { useBuildVersion, useLloaModelOptions } from "../../hooks/build.query";
import { useClauseLabelOverride } from "../../hooks/build.mutation";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
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

// 드릴다운 selector의 "전체" 항목 sentinel key (실제 aspect key와 충돌 방지).
const ALL_KEY = "__all__";

const SENTIMENT_FILTER_OPTIONS: { label: string; value: string | "" }[] = [
  { label: "전체", value: "" },
  { label: "긍정", value: "positive" },
  { label: "중립", value: "neutral" },
  { label: "부정", value: "negative" },
];

// 교차검증(verify, ADR-028) resolution 한글 라벨.
const RESOLUTION_LABEL: Record<string, string> = {
  agree: "모델 합의",
  union: "aspect 통합",
  sentiment_auto: "감성 자동",
  judge: "judge 결정",
  needs_review: "검토 필요",
  partial_classify: "부분 분류",
};

function SentimentBadge({ value }: { value: string }) {
  return (
    <Badge className={SENTIMENT_BADGE[value as Sentiment]}>
      {SENTIMENT_LABELS[value as Sentiment]}
    </Badge>
  );
}

// 액션 컬럼 아이콘 버튼.
function IconBtn({
  onClick,
  title,
  disabled,
  className,
  children,
}: {
  onClick: () => void;
  title: string;
  disabled?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      disabled={disabled}
      className={cn(
        "inline-grid h-7 w-7 place-items-center rounded-md text-zinc-400 transition-colors hover:bg-zinc-100 hover:text-zinc-700 disabled:opacity-40",
        className,
      )}
    >
      {children}
    </button>
  );
}

export function ClauseTab() {
  const [filter, setFilter] = useState<string | "">("");
  const [aspectFilter, setAspectFilter] = useState<string>("all");
  // 교차검증 검토 큐 필터 (ADR-028): "" | "disagreement" | "needs_review".
  const [reviewFilter, setReviewFilter] = useState<string>("");
  const [page, setPage] = useState(1);
  // 드릴다운: 선택된 aspect key (null이면 건수 1위 aspect로 fallback)
  const [activeAspect, setActiveAspect] = useState<string | null>(null);
  const pageSize = 10;

  // 교차검증 배너의 모델 id→표시명 변환 (allowlist).
  const { data: lloaModels = [] } = useLloaModelOptions();
  const modelLabelOf = (id?: string) =>
    lloaModels.find((m) => m.model_id === id)?.label ?? id ?? "";

  // 절 aspect/sentiment 수동 보정 — row 단위 inline edit.
  const override = useClauseLabelOverride();
  const [editingClauseId, setEditingClauseId] = useState<string | null>(null);
  const [draftAspect, setDraftAspect] = useState<string>("");
  const [draftSentiment, setDraftSentiment] = useState<string>("neutral");
  const [savingClauseId, setSavingClauseId] = useState<string | null>(null);

  // 서버 페이징 + 서버 필터: 표는 서버가 필터/페이징해 준 현재 페이지(items)만 렌더.
  const { data, isLoading, isPlaceholderData } = useBuildVersion(
    "clause_label",
    undefined,
    {
      limit: pageSize,
      offset: (page - 1) * pageSize,
      aspect: aspectFilter === "all" ? undefined : aspectFilter,
      sentiment: filter || undefined,
      disagreement: reviewFilter === "disagreement" || undefined,
      needs_review: reviewFilter === "needs_review" || undefined,
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
    // 이전 빌드 결과(summary)가 없는 첫 실행/실패-후 실행. 진행 중이면 경과시간 +
    // 이번 실행 프롬프트(백엔드가 in-flight job에서 내려줌)를 메타 행으로 보여준다.
    // 완료 artifact가 없어 결과 표는 아직 없으므로 메타 + 진행 배너만 렌더한다.
    return isBuildRunning(status) ? (
      <div className="space-y-5">
        <BuildMetaBar
          status={status}
          durationSeconds={durationSeconds}
          applied={applied}
        />
        <BuildRunningBanner
          status={status}
          progress={progress}
          hasPrevious={false}
        />
      </div>
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

  // 수정 드롭다운용 aspect 키 — taxonomy 전체(9종) 우선, 없으면 관측된 분포 키.
  const aspectEditKeys = taxonomy
    ? Object.keys(taxonomy.aspectLabels)
    : aspectOptions;

  // pagination.total은 (필터 적용된) 전체 건수. 표/페이지 계산의 기준.
  const totalCount = pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  // 교차검증(verify) 모드 — 검토 큐 필터/배너/검증 컬럼 노출.
  const isVerify = summary.mode === "verify";

  // classify 모델 1개 결과를 "감성 / aspect" 형태로 요약. 무관/미분류 분기.
  const fmtModelResult = (r?: ClauseModelResult | null): string => {
    if (!r) return "—";
    if (r.relevant === false) return "무관";
    const sent = SENTIMENT_LABELS[r.sentiment as Sentiment] ?? r.sentiment ?? "";
    const asp = (r.aspects ?? []).map((a) => aspectLabelOf(taxonomy, a)).join("·");
    return [sent, asp].filter(Boolean).join(" / ") || "—";
  };

  function startEdit(item: ClauseItem) {
    if (savingClauseId) return;
    setEditingClauseId(item.clauseId);
    setDraftAspect(item.aspect);
    setDraftSentiment(item.sentiment);
  }
  function cancelEdit() {
    setEditingClauseId(null);
  }
  async function saveEdit(item: ClauseItem) {
    if (savingClauseId) return;
    // 변경된 필드만 전송. 둘 다 그대로면 저장 없이 편집만 종료.
    const aspect = draftAspect !== item.aspect ? draftAspect : undefined;
    const sentiment =
      draftSentiment !== item.sentiment ? draftSentiment : undefined;
    if (aspect === undefined && sentiment === undefined) {
      setEditingClauseId(null);
      return;
    }
    setSavingClauseId(item.clauseId);
    try {
      await override.set.mutateAsync({ clauseId: item.clauseId, aspect, sentiment });
      setEditingClauseId(null);
    } finally {
      setSavingClauseId(null);
    }
  }
  async function revertOverride(clauseId: string) {
    if (savingClauseId) return;
    setSavingClauseId(clauseId);
    try {
      await override.remove.mutateAsync({ clauseId });
      if (editingClauseId === clauseId) setEditingClauseId(null);
    } finally {
      setSavingClauseId(null);
    }
  }

  const columns: Column<ClauseItem>[] = [
    {
      header: "문장",
      headerClassName: "w-80",
      cell: (item) => <ExpandableTextCell text={item.clause} />,
    },
    {
      header: "Aspect",
      headerClassName: "w-36",
      cell: (item) => {
        const editing = editingClauseId === item.clauseId;
        if (editing) {
          return (
            <td className="px-4 py-3">
              <Select
                value={draftAspect}
                onValueChange={setDraftAspect}
                disabled={savingClauseId === item.clauseId}
              >
                <SelectTrigger className="h-7 w-32 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {aspectEditKeys.map((key) => (
                    <SelectItem key={key} value={key} className="text-xs">
                      {aspectLabelOf(taxonomy, key)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </td>
          );
        }
        return (
          <td className="px-4 py-3">
            <div className="flex items-center gap-1.5">
              <span className="text-xs text-zinc-500">
                {aspectLabelOf(taxonomy, item.aspect)}
              </span>
              {item.isOverridden && (
                <span
                  title={
                    item.originalAspect
                      ? `원본: ${aspectLabelOf(taxonomy, item.originalAspect)} / ${SENTIMENT_LABELS[item.originalSentiment as Sentiment] ?? item.originalSentiment}`
                      : "수동 수정됨"
                  }
                  className="rounded-full bg-amber-50 px-1.5 py-0.5 text-[10px] font-semibold text-amber-600"
                >
                  수정됨
                </span>
              )}
            </div>
          </td>
        );
      },
    },
    {
      header: "감성",
      headerClassName: "w-28",
      cell: (item) => {
        const editing = editingClauseId === item.clauseId;
        if (editing) {
          return (
            <td className="px-4 py-3">
              <Select
                value={draftSentiment}
                onValueChange={setDraftSentiment}
                disabled={savingClauseId === item.clauseId}
              >
                <SelectTrigger className="h-7 w-24 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {SENTIMENT_ORDER.map((s) => (
                    <SelectItem key={s} value={s} className="text-xs">
                      {SENTIMENT_LABELS[s]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </td>
          );
        }
        return (
          <td className="px-4 py-3">
            <SentimentBadge value={item.sentiment} />
          </td>
        );
      },
    },
    ...(isVerify
      ? [
          {
            header: "검증",
            headerClassName: "w-72",
            cell: (item: ClauseItem) => (
              <td className="px-4 py-3 align-top text-[11px] leading-relaxed">
                {item.resolution ? (
                  <div className="space-y-1">
                    <div className="flex flex-wrap items-center gap-1">
                      <span className="rounded-full bg-zinc-100 px-1.5 py-0.5 font-semibold text-zinc-600">
                        {RESOLUTION_LABEL[item.resolution] ?? item.resolution}
                      </span>
                      {item.needsReview && (
                        <span className="rounded-full bg-amber-100 px-1.5 py-0.5 font-semibold text-amber-700">
                          검토 필요
                        </span>
                      )}
                      {typeof item.chunkIndex === "number" &&
                        item.chunkIndex > 0 && (
                          <span
                            className="rounded-full bg-sky-50 px-1.5 py-0.5 font-medium text-sky-600"
                            title="긴 문서 chunk 분할 (ADR-029)"
                          >
                            chunk {item.chunkIndex}
                          </span>
                        )}
                    </div>
                    {item.resolution !== "agree" &&
                      (item.modelAResult || item.modelBResult) && (
                        <div className="rounded-md bg-zinc-50 px-2 py-1 text-zinc-600">
                          <span>
                            모델 A: <b>{fmtModelResult(item.modelAResult)}</b> · 모델 B:{" "}
                            <b>{fmtModelResult(item.modelBResult)}</b>
                          </span>
                          {item.judgeResult && (
                            <span>
                              {" "}→ judge: <b>{fmtModelResult(item.judgeResult)}</b>
                              {item.judgeResult.reason && (
                                <span className="text-zinc-400">
                                  {" "}({item.judgeResult.reason})
                                </span>
                              )}
                            </span>
                          )}
                        </div>
                      )}
                  </div>
                ) : (
                  <span className="text-zinc-300">—</span>
                )}
              </td>
            ),
          } as Column<ClauseItem>,
        ]
      : []),
    {
      header: "",
      headerClassName: "w-20 text-right",
      cell: (item) => {
        const editing = editingClauseId === item.clauseId;
        const saving = savingClauseId === item.clauseId;
        return (
          <td className="px-4 py-3">
            <div className="flex items-center justify-end gap-0.5">
              {editing ? (
                <>
                  <IconBtn
                    onClick={() => saveEdit(item)}
                    title="저장"
                    disabled={saving}
                    className="text-violet-600 hover:bg-violet-50 hover:text-violet-700"
                  >
                    {saving ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Check className="h-4 w-4" />
                    )}
                  </IconBtn>
                  <IconBtn onClick={cancelEdit} title="취소" disabled={saving}>
                    <X className="h-4 w-4" />
                  </IconBtn>
                </>
              ) : (
                <>
                  <IconBtn
                    onClick={() => startEdit(item)}
                    title="aspect/감성 수정"
                    disabled={!!savingClauseId}
                  >
                    {saving ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Pencil className="h-4 w-4" />
                    )}
                  </IconBtn>
                  {item.isOverridden && (
                    <IconBtn
                      onClick={() => revertOverride(item.clauseId)}
                      title="원본 라벨로 되돌리기"
                      disabled={!!savingClauseId}
                    >
                      <RotateCcw className="h-4 w-4" />
                    </IconBtn>
                  )}
                </>
              )}
            </div>
          </td>
        );
      },
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

      {/* 교차검증(verify) 요약 배너 (ADR-028) */}
      {isVerify && (
        <div className="rounded-2xl border border-violet-200 bg-violet-50/50 p-4 text-sm">
          <div className="font-bold text-violet-800">교차검증 결과</div>
          <p className="mt-1 text-zinc-700">
            {(() => {
              const rc = summary.resolution ?? {};
              const agree = rc["agree"] ?? 0;
              const judge = rc["judge"] ?? 0;
              const review = rc["needs_review"] ?? 0;
              const reconciled =
                (rc["union"] ?? 0) +
                (rc["sentiment_auto"] ?? 0) +
                judge +
                (rc["partial_classify"] ?? 0) +
                review;
              return (
                <>
                  모델 합의 <b>{agree.toLocaleString()}</b>건 · 재조정{" "}
                  <b>{reconciled.toLocaleString()}</b>건(judge 처리{" "}
                  {judge.toLocaleString()}건) · 검토 필요{" "}
                  <b>{review.toLocaleString()}</b>건.
                </>
              );
            })()}
          </p>
          {summary.models && (
            <p className="mt-1 text-xs text-zinc-500">
              모델 A = {modelLabelOf(summary.models.a)}, 모델 B ={" "}
              {modelLabelOf(summary.models.b)}
              {summary.models.judge && (
                <> · judge = {modelLabelOf(summary.models.judge)}</>
              )}
            </p>
          )}
          <p className="mt-1 text-[11px] text-zinc-400">
            두 모델이 같은 문장을 라벨링해 합의는 신뢰 신호, 갈린 절만 judge가 라벨
            기준으로 재검토합니다. judge 미해소 절은 검토 필요로 격리됩니다.
          </p>
        </div>
      )}

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
            {isVerify && (
              <FilterPills
                options={[
                  { label: "전체", value: "" },
                  { label: "불일치만", value: "disagreement" },
                  { label: "검토 필요", value: "needs_review" },
                ]}
                value={reviewFilter}
                onChange={(value) => {
                  setReviewFilter(value);
                  setPage(1);
                }}
              />
            )}
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
