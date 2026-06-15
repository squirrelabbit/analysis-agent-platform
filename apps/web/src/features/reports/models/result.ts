import type {
  AnalysisPlanDto,
  ChatChart,
  ChatDisplay,
  ChatEvidence,
  ChatEvidenceItem,
  ChatMetric,
  ChatPlan,
  ColumnFormat,
  ComposerDisplayDto,
  RecommendedView,
} from "@/features/chats/models";
import { toColumnFormat } from "@/features/chats/models";
import type { ReportSavedResult } from "./model";

// saved_results의 display/plan(서버 raw)을 채팅과 동일한 도메인(ChatChart/Metric/Evidence/
// Display/Plan)으로 투영한다. 채팅 결과 뷰 카탈로그(ChartView 등)를 그대로 재사용하기 위함.
// 투영 규칙은 채팅 매퍼와 동일하게 맞춘다(chats 파일은 수정하지 않으므로 reports에 재현).

const MIN_CHART_DATA_POINTS = 2;
const CHART_KINDS = new Set(["bar", "line", "diverging_bar"]);
const KNOWN_VIEWS = new Set([
  "table",
  "bar",
  "diverging_bar",
  "line",
  "metric",
  "evidence",
]);

const isNumericLike = (v: unknown): boolean => {
  if (typeof v === "number") return Number.isFinite(v);
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) && v.trim() !== "";
  }
  return false;
};

const toNum = (v: unknown): number | null => {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
};

const mapColumnFormats = (
  raw: Record<string, string> | null | undefined,
): Record<string, ColumnFormat> | undefined => {
  if (!raw) return undefined;
  const out: Record<string, ColumnFormat> = {};
  for (const [col, fmt] of Object.entries(raw)) {
    const f = toColumnFormat(fmt);
    if (f) out[col] = f;
  }
  return Object.keys(out).length > 0 ? out : undefined;
};

const mapDisplay = (
  dto: ComposerDisplayDto | undefined,
): ChatDisplay | undefined => {
  if (!dto || dto.type !== "table") return undefined;
  const columns = dto.columns ?? [];
  const rows = dto.rows ?? [];
  if (columns.length === 0) return undefined;
  return {
    type: "table",
    title: dto.title ?? undefined,
    columns,
    rows,
    columnFormats: mapColumnFormats(dto.column_formats),
    columnLabels: dto.column_labels ?? undefined,
  };
};

const mapChart = (dto: ComposerDisplayDto | undefined): ChatChart | undefined => {
  if (!dto) return undefined;
  const view = dto.recommended_view;
  const spec = dto.chart_spec;
  if (view !== "bar" && view !== "line" && view !== "diverging_bar")
    return undefined;
  if (!spec || !CHART_KINDS.has(spec.kind)) return undefined;
  const rows = dto.rows ?? [];
  if (rows.length === 0) return undefined;
  const x = spec.x;
  const yKey = Array.isArray(spec.y) ? spec.y[0] : spec.y;
  if (!x || !yKey) return undefined;
  const validPoints = rows.filter((row) => isNumericLike(row[yKey])).length;
  if (validPoints < MIN_CHART_DATA_POINTS) return undefined;
  return {
    kind: spec.kind as ChatChart["kind"],
    x,
    y: yKey,
    title: dto.title ?? undefined,
    rows,
    yFormat: toColumnFormat(dto.column_formats?.[yKey]),
    yLabel: dto.column_labels?.[yKey],
    unit: spec.unit ?? undefined,
    countKey: spec.count_col ?? undefined,
    eventDate: spec.event_date ?? undefined,
    eventLabel: spec.event_label ?? undefined,
  };
};

const mapRecommendedView = (
  dto: ComposerDisplayDto | undefined,
): RecommendedView | undefined => {
  const v = dto?.recommended_view;
  if (!v) return undefined;
  return KNOWN_VIEWS.has(v) ? (v as RecommendedView) : "unknown";
};

const mapMetric = (
  dto: ComposerDisplayDto | undefined,
): ChatMetric | undefined => {
  if (!dto || dto.recommended_view !== "metric") return undefined;
  const spec = dto.chart_spec;
  if (!spec || spec.kind !== "metric") return undefined;
  const row = (dto.rows ?? [])[0];
  if (!row) return undefined;
  const get = (col?: string) => (col ? toNum(row[col]) : null);
  return {
    aValue: get(spec.a_value),
    bValue: get(spec.b_value),
    deltaValue: get(spec.delta_value),
    deltaRate: get(spec.delta_rate),
    unit: spec.unit ?? "",
  };
};

const mapEvidence = (
  dto: ComposerDisplayDto | undefined,
): ChatEvidence | undefined => {
  if (!dto || dto.recommended_view !== "evidence") return undefined;
  const spec = dto.chart_spec;
  if (!spec || spec.kind !== "evidence" || !spec.text) return undefined;
  const rows = dto.rows ?? [];
  if (rows.length === 0) return undefined;
  const textCol = spec.text;
  const chipCols = spec.chips ?? [];
  const items = rows
    .map((row): ChatEvidenceItem => ({
      text: String(row[textCol] ?? "").trim(),
      sentiment: spec.sentiment
        ? String(row[spec.sentiment] ?? "").trim() || undefined
        : undefined,
      chips: chipCols
        .map((c) => ({ key: c, value: String(row[c] ?? "").trim() }))
        .filter((c) => c.value),
      id: spec.id ? String(row[spec.id] ?? "").trim() || undefined : undefined,
    }))
    .filter((it) => it.text);
  if (items.length === 0) return undefined;
  return { items, total: rows.length };
};

const mapPlan = (dto: AnalysisPlanDto | undefined): ChatPlan | undefined => {
  if (!dto || !dto.steps?.length) return undefined;
  return {
    version: dto.plan_version,
    steps: dto.steps.map((s) => ({
      id: s.id,
      skill: s.skill,
      params: s.params ?? {},
      label: s.display?.label?.trim() || undefined,
      expression: s.display?.expression?.trim() || undefined,
    })),
  };
};

// 채팅 결과 뷰가 쓰는 도메인 묶음. 렌더 선택은 ReportBlock에서 metric>evidence>chart>table.
export interface ReportResult {
  recommendedView?: RecommendedView;
  chart?: ChatChart;
  metric?: ChatMetric;
  evidence?: ChatEvidence;
  display?: ChatDisplay;
  plan?: ChatPlan;
}

export const projectResult = (r: ReportSavedResult): ReportResult => ({
  recommendedView: mapRecommendedView(r.display),
  chart: mapChart(r.display),
  metric: mapMetric(r.display),
  evidence: mapEvidence(r.display),
  display: mapDisplay(r.display),
  plan: mapPlan(r.plan),
});
