import type {
  AnalysisMessageDto,
  AnalysisPlanDto,
  AnalysisThreadDetailDto,
  AnalysisThreadDto,
  AnalysisThreadMessageResponseDto,
  ComposerDisplayDto,
  TaxonomyCheckDto,
} from "./dto";
import type {
  AnalyzeResult,
  ChatChart,
  ChatDisplay,
  ChatMessage,
  ChatPlan,
  ChatThread,
  ChatThreadDetail,
  RecommendedView,
  RunStatus,
  TaxonomyStatus,
} from "./model";
import type { ColumnFormat } from "./format";
import { toColumnFormat } from "./format";

// 유효 numeric value를 가진 row가 N개 이상일 때만 chart로 표시한다.
// 한두 개 row만 numeric인 경우 막대가 하나만 그려져 오해 소지가 큼.
const MIN_CHART_DATA_POINTS = 2;

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

const mapDisplay = (dto: ComposerDisplayDto | undefined): ChatDisplay | undefined => {
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

// recommended_view가 bar/line이고 chart_spec/rows가 유효할 때만 chart로 노출한다.
// y가 array면 첫 값을 사용해 단일 series로 좁힌다 (사용자 명시 1차 정책).
// 추가: 유효 numeric data point가 MIN_CHART_DATA_POINTS 미만이면 chart 생략
// (사용자 정책 — 막대가 1개만 그려져 오해 소지 큼).
const isNumericLike = (v: unknown): boolean => {
  if (typeof v === "number") return Number.isFinite(v);
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) && v.trim() !== "";
  }
  return false;
};

const CHART_KINDS = new Set(["bar", "line", "diverging_bar"]);

const mapChart = (dto: ComposerDisplayDto | undefined): ChatChart | undefined => {
  if (!dto) return undefined;
  const view = dto.recommended_view;
  const spec = dto.chart_spec;
  if (view !== "bar" && view !== "line" && view !== "diverging_bar") return undefined;
  if (!spec || !CHART_KINDS.has(spec.kind)) return undefined;
  const rows = dto.rows ?? [];
  if (rows.length === 0) return undefined;
  const x = spec.x;
  const yKey = Array.isArray(spec.y) ? spec.y[0] : spec.y;
  if (!x || !yKey) return undefined;
  const validPoints = rows.filter((row) => isNumericLike(row[yKey])).length;
  if (validPoints < MIN_CHART_DATA_POINTS) return undefined;
  const yFormat = toColumnFormat(dto.column_formats?.[yKey]);
  const yLabel = dto.column_labels?.[yKey];
  return {
    kind: spec.kind as ChatChart["kind"],
    x,
    y: yKey,
    title: dto.title ?? undefined,
    rows,
    yFormat,
    yLabel,
    unit: spec.unit ?? undefined,
  };
};

// 알려진 view만 좁혀 반환. 정의된 외 값(metric 등 미래 확장)이면 unknown.
const mapRecommendedView = (
  dto: ComposerDisplayDto | undefined,
): RecommendedView | undefined => {
  const v = dto?.recommended_view;
  if (!v) return undefined;
  if (v === "table" || v === "bar" || v === "diverging_bar" || v === "line") return v;
  return "unknown";
};

const mapRunStatus = (status: string | undefined): RunStatus | undefined => {
  if (status === "running" || status === "completed" || status === "failed") {
    return status;
  }
  return undefined;
};

const mapTaxonomyStatus = (
  dto: TaxonomyCheckDto | undefined,
): TaxonomyStatus | undefined => {
  const s = dto?.status;
  if (s === "ok" || s === "legacy_missing" || s === "hash_mismatch" || s === "id_mismatch") {
    return s;
  }
  return undefined;
};

export const mapAnalyzeResponse = (
  dto: AnalysisThreadMessageResponseDto,
): AnalyzeResult => {
  // 사용자 명시: assistant 텍스트는 result.composer.assistant_content 기준,
  // 없으면 assistant_message.content로 폴백.
  const content =
    dto.result?.composer?.assistant_content ?? dto.assistant_message?.content;

  const display = dto.result?.composer?.display;
  const chart = mapChart(display);
  const recommendedView = mapRecommendedView(display);
  // 백엔드가 bar/line을 추천했지만 mapChart가 생성을 포기한 경우 → fallback 안내.
  const chartFallbackReason: ChatMessage["chartFallbackReason"] =
    (recommendedView === "bar" || recommendedView === "line") && !chart
      ? "insufficient_data"
      : undefined;
  const assistantMessage: ChatMessage | undefined =
    content !== undefined && dto.assistant_message
      ? {
          id: dto.assistant_message.message_id,
          role: "assistant",
          content,
          createdAt: dto.assistant_message.created_at,
          display: mapDisplay(display),
          chart,
          warnings: display?.warnings?.length ? display.warnings : undefined,
          taxonomyStatus: mapTaxonomyStatus(dto.result?.taxonomy_check),
          plan: mapPlan(dto.result?.plan),
          recommendedView,
          chartFallbackReason,
          runStatus: mapRunStatus(dto.run?.status),
          runError: dto.run?.error_message?.trim() || undefined,
        }
      : undefined;

  const userMessage: ChatMessage = {
    id: dto.user_message.message_id,
    role: "user",
    content: dto.user_message.content,
    createdAt: dto.user_message.created_at,
  };

  return {
    threadId: dto.thread_id,
    userMessage,
    assistantMessage,
    errorMessage: dto.run?.error_message ?? null,
  };
};

export const mapThread = (dto: AnalysisThreadDto): ChatThread => ({
  id: dto.thread_id,
  title: dto.title?.trim() || "제목 없음",
  lastMessage: dto.last_message ?? "",
  messageCount: dto.message_count ?? 0,
  updatedAt: dto.updated_at,
});

// thread detail의 assistant messages에는 composer.display projection이
// 동봉되므로 POST 응답과 동일하게 mapDisplay/mapChart/warnings로 매핑한다.
// taxonomy_check은 이력에 보존되지 않아 표시 안 함.
const mapStoredMessage = (dto: AnalysisMessageDto): ChatMessage => {
  const chart = mapChart(dto.display);
  const recommendedView = mapRecommendedView(dto.display);
  const chartFallbackReason: ChatMessage["chartFallbackReason"] =
    (recommendedView === "bar" || recommendedView === "line") && !chart
      ? "insufficient_data"
      : undefined;
  return {
    id: dto.message_id,
    role: dto.role,
    content: dto.content,
    createdAt: dto.created_at,
    display: mapDisplay(dto.display),
    chart,
    warnings: dto.display?.warnings?.length ? dto.display.warnings : undefined,
    recommendedView,
    chartFallbackReason,
    plan: mapPlan(dto.plan),
  };
};

export const mapThreadDetail = (dto: AnalysisThreadDetailDto): ChatThreadDetail => ({
  id: dto.thread_id,
  messages: dto.messages.map(mapStoredMessage),
});
