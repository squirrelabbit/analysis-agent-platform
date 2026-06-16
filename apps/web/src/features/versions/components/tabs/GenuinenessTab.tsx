import {
  AlertTriangle,
  Check,
  FileText,
  Loader2,
  Minus,
  Pencil,
  RotateCcw,
  X,
  type LucideIcon,
} from "lucide-react";
import type { GenuinenessBuild, GenuinenessItem } from "../../models/build";
import { useState, type ReactNode } from "react";
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
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useBuildVersion, useLloaModelOptions } from "../../hooks/build.query";
import { useGenuinenessOverride } from "../../hooks/build.mutation";
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

// 필터 옵션: "전체" + 진성 3분류 (라벨은 GENUINENESS_LABELS 단일 출처).
const FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  ...GENUINENESS_ORDER.map((key) => ({
    label: GENUINENESS_LABELS[key],
    value: key,
  })),
];

function labelOf(value: string): string {
  return GENUINENESS_LABELS[value as Genuineness] ?? value;
}
function badgeClassOf(value: string): string {
  return GENUINENESS_BADGE[value as Genuineness] ?? "bg-zinc-100 text-zinc-500";
}

export function GenuinenessBadge({ value }: { value: string }) {
  // min-w-14로 가장 긴 라벨(비진성/불확실, 3글자) 폭에 맞춰 동일 폭 + 가운데 정렬.
  return (
    <Badge className={cn("min-w-14", badgeClassOf(value))}>{labelOf(value)}</Badge>
  );
}

// 인라인 라벨 드롭다운 — 편집 중 판별 결과 셀에 노출.
function GenuinenessSelect({
  value,
  onChange,
  disabled,
}: {
  value: Genuineness;
  onChange: (v: Genuineness) => void;
  disabled?: boolean;
}) {
  return (
    <Select
      value={value}
      onValueChange={(v) => onChange(v as Genuineness)}
      disabled={disabled}
    >
      <SelectTrigger className="mx-auto h-7 w-24 text-xs">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {GENUINENESS_ORDER.map((tier) => (
          <SelectItem key={tier} value={tier} className="text-xs">
            {GENUINENESS_LABELS[tier]}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
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

export default function GenuinenessTab() {
  const [filter, setFilter] = useState<string>("");
  // 교차검증 검토 큐 필터 (ADR-026): "" | "disagreement" | "needs_review".
  const [reviewFilter, setReviewFilter] = useState<string>("");
  const [page, setPage] = useState(1);
  const pageSize = 10;

  // 교차검증 배너의 모델 id→표시명 변환 (allowlist).
  const { data: lloaModels = [] } = useLloaModelOptions();
  const modelLabelOf = (id?: string) =>
    lloaModels.find((m) => m.model_id === id)?.label ?? id ?? "";

  // 진성 라벨 수동 보정 — row 단위 inline edit. editingDocId가 편집 중 row,
  // draft*는 입력값, savingDocId는 저장 진행 중(연타·중복 방지).
  const override = useGenuinenessOverride();
  const [editingDocId, setEditingDocId] = useState<string | null>(null);
  const [draftGenuineness, setDraftGenuineness] =
    useState<Genuineness>("genuine_review");
  const [draftReason, setDraftReason] = useState("");
  const [savingDocId, setSavingDocId] = useState<string | null>(null);

  function startEdit(item: GenuinenessItem) {
    if (savingDocId) return;
    setEditingDocId(item.docId);
    setDraftGenuineness((item.genuineness as Genuineness) ?? "genuine_review");
    setDraftReason(item.overrideReason ?? "");
  }
  function cancelEdit() {
    setEditingDocId(null);
    setDraftReason("");
  }
  async function saveEdit(docId: string) {
    if (savingDocId) return;
    setSavingDocId(docId);
    try {
      await override.set.mutateAsync({
        docId,
        genuineness: draftGenuineness,
        reason: draftReason.trim() || undefined,
      });
      setEditingDocId(null);
      setDraftReason("");
    } finally {
      setSavingDocId(null);
    }
  }
  async function revertOverride(docId: string) {
    if (savingDocId) return;
    setSavingDocId(docId);
    try {
      await override.remove.mutateAsync({ docId });
      if (editingDocId === docId) setEditingDocId(null);
    } finally {
      setSavingDocId(null);
    }
  }

  const columns: Column<GenuinenessItem>[] = [
    {
      header: "정제 텍스트",
      cell: (item) => <ExpandableTextCell text={item.cleanedText} />,
    },
    {
      header: "판별 결과",
      headerClassName: "w-48 text-center",
      cell: (item) => {
        const editing = editingDocId === item.docId;
        return (
          <td className="px-4 py-3 text-center">
            {editing ? (
              <GenuinenessSelect
                value={draftGenuineness}
                onChange={setDraftGenuineness}
                disabled={savingDocId === item.docId}
              />
            ) : !item.genuineness &&
              (item.resolution === "classify_error" ||
                item.resolution === "judge_error") ? (
              <span className="rounded-full bg-rose-50 px-2 py-0.5 text-[11px] font-semibold text-rose-600">
                분류 실패
              </span>
            ) : (
              <div className="flex items-center justify-center gap-1.5">
                <GenuinenessBadge value={item.genuineness} />
                {item.isOverridden && (
                  <span
                    title={
                      item.originalGenuineness
                        ? `원본: ${labelOf(item.originalGenuineness)}`
                        : "수동 수정됨"
                    }
                    className="rounded-full bg-amber-50 px-1.5 py-0.5 text-[10px] font-semibold text-amber-600"
                  >
                    수정됨
                  </span>
                )}
              </div>
            )}
          </td>
        );
      },
    },
    {
      header: "사유",
      cell: (item) => {
        const editing = editingDocId === item.docId;
        if (editing) {
          return (
            <td className="px-4 py-3">
              <Input
                value={draftReason}
                onChange={(e) => setDraftReason(e.target.value)}
                placeholder="수정 사유 (선택 · 비우면 '운영자 수동 수정')"
                disabled={savingDocId === item.docId}
                className="h-8 text-xs"
              />
            </td>
          );
        }
        return (
          <td className="px-4 py-3 text-xs leading-relaxed max-w-sm">
            <div className="text-zinc-600">{item.reason}</div>
            {item.isOverridden && item.originalReason && (
              <div className="mt-1 text-[11px] text-zinc-400">
                원본 판정: {item.originalReason}
              </div>
            )}
            {/* 교차검증(verify) 상세 — resolution 기준 분기 (ADR-026) */}
            {(item.resolution === "classify_error" ||
              item.resolution === "judge_error") && (
              <div className="mt-1.5 rounded-md bg-rose-50 px-2 py-1 text-[11px] text-rose-700">
                분류 실패 — 두 모델 모두 분류에 실패해 불확실로 처리했습니다. 재실행을 권장합니다.
                <span className="ml-1 rounded-full bg-rose-100 px-1.5 py-0.5 font-semibold">
                  검토 필요
                </span>
              </div>
            )}
            {item.resolution === "partial_classify" && (
              <div className="mt-1.5 rounded-md bg-amber-50 px-2 py-1 text-[11px] text-amber-800">
                한 모델 분류 실패 — 단일 모델 결과입니다(교차검증 미완). 재실행을 권장합니다.
                <span className="ml-1 rounded-full bg-amber-100 px-1.5 py-0.5 font-semibold">
                  검토 필요
                </span>
              </div>
            )}
            {item.resolution === "judge_on_disagreement" && item.modelAResult && (
              <div className="mt-1.5 rounded-md bg-amber-50/70 px-2 py-1 text-[11px] text-amber-800">
                <span>
                  모델 A: <b>{labelOf(item.modelAResult.genuineness ?? "")}</b> · 모델 B:{" "}
                  <b>{labelOf(item.modelBResult?.genuineness ?? "")}</b>
                </span>
                {item.judgeResult && (
                  <span>
                    {" "}→ judge: <b>{labelOf(item.judgeResult.finalLabel ?? "")}</b>
                    {typeof item.judgeResult.confidence === "number" && (
                      <span
                        className="text-amber-600"
                        title="judge 자기보고 신뢰도 — 정답 확률 아님"
                      >
                        {" "}(판정 신뢰도 {item.judgeResult.confidence.toFixed(2)})
                      </span>
                    )}
                  </span>
                )}
                {item.needsReview && (
                  <span className="ml-1 rounded-full bg-amber-100 px-1.5 py-0.5 font-semibold text-amber-700">
                    검토 필요
                  </span>
                )}
              </div>
            )}
            {item.resolution === "model_agreement" && (
              <div className="mt-1.5 text-[11px] text-zinc-400">모델 합의</div>
            )}
          </td>
        );
      },
    },
    {
      header: "",
      headerClassName: "w-20 text-right",
      cell: (item) => {
        const editing = editingDocId === item.docId;
        const saving = savingDocId === item.docId;
        return (
          <td className="px-4 py-3">
            <div className="flex items-center justify-end gap-0.5">
              {editing ? (
                <>
                  <IconBtn
                    onClick={() => saveEdit(item.docId)}
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
                    title="진성 라벨 수정"
                    disabled={!!savingDocId}
                  >
                    {saving ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Pencil className="h-4 w-4" />
                    )}
                  </IconBtn>
                  {item.isOverridden && (
                    <IconBtn
                      onClick={() => revertOverride(item.docId)}
                      title="원본 판정으로 되돌리기"
                      disabled={!!savingDocId}
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

  // 서버 페이징 + 서버 필터: 표는 서버가 필터/페이징해 준 현재 페이지(items)만 렌더.
  const { data, isLoading, isPlaceholderData } = useBuildVersion(
    "doc_genuineness",
    undefined,
    {
      limit: pageSize,
      offset: (page - 1) * pageSize,
      genuineness: filter || undefined,
      disagreement: reviewFilter === "disagreement" || undefined,
      needs_review: reviewFilter === "needs_review" || undefined,
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

      {/* 교차검증(verify) 요약 배너 (ADR-026) */}
      {summary.mode === "verify" && (
        <div className="rounded-2xl border border-violet-200 bg-violet-50/50 p-4 text-sm">
          <div className="font-bold text-violet-800">교차검증 결과</div>
          <p className="mt-1 text-zinc-700">
            모델 합의 <b>{(summary.agreementCount ?? 0).toLocaleString()}</b>건 · 불일치{" "}
            <b>{(summary.disagreementCount ?? 0).toLocaleString()}</b>건(judge 처리{" "}
            {(summary.judgeCount ?? 0).toLocaleString()}건) · 검토 필요{" "}
            <b>{(summary.reviewCount ?? 0).toLocaleString()}</b>건.
          </p>
          {Array.isArray(
            (applied as Record<string, unknown> | undefined)?.["classify_models"],
          ) && (
            <p className="mt-1 text-xs text-zinc-500">
              모델 A ={" "}
              {modelLabelOf(
                ((applied as Record<string, unknown>)["classify_models"] as string[])[0],
              )}
              , 모델 B ={" "}
              {modelLabelOf(
                ((applied as Record<string, unknown>)["classify_models"] as string[])[1],
              )}
              {(applied as Record<string, unknown>)["judge_model"] ? (
                <>
                  {" "}· judge ={" "}
                  {modelLabelOf(
                    (applied as Record<string, unknown>)["judge_model"] as string,
                  )}
                </>
              ) : null}
            </p>
          )}
          <p className="mt-1 text-[11px] text-zinc-400">
            합의는 신뢰 신호, 불일치는 judge가 라벨 기준으로 재검토합니다. judge 신뢰도는
            모델 자기보고 값이며 정답 확률이 아닙니다.
          </p>
        </div>
      )}

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

      {/* 수동 보정이 clause_label 포함 경계를 넘고 후속 단계가 이미 생성돼 있으면
          재실행 권장 (자동 재실행은 하지 않음). */}
      {summary.downstreamRerunRecommended && (
        <div className="flex items-start gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-700">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            진성 라벨을 수정해 후속 분석 대상이 바뀌었습니다. 절·감성·키워드
            분석에 반영하려면 <b>절 라벨링(clause_label)을 다시 실행</b>하세요.
            (수정 사항은 자동 재실행되지 않습니다.)
          </span>
        </div>
      )}

      {/* Table */}
      <DataTable
        columns={columns}
        items={items}
        rowKey={(item) => item.docId}
        title={`판별 결과 상세`}
        toolbar={
          <div className="flex flex-wrap items-center gap-2">
            <FilterPills
              options={FILTER_OPTIONS}
              value={filter}
              onChange={(value) => {
                setFilter(value);
                setPage(1);
              }}
            />
            {summary.mode === "verify" && (
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
          </div>
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
