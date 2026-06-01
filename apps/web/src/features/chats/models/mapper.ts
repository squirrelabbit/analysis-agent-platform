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
  TaxonomyStatus,
} from "./model";

const mapPlan = (dto: AnalysisPlanDto | undefined): ChatPlan | undefined => {
  if (!dto || !dto.steps?.length) return undefined;
  return {
    version: dto.plan_version,
    steps: dto.steps.map((s) => ({
      id: s.id,
      skill: s.skill,
      params: s.params ?? {},
    })),
  };
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
  };
};

// recommended_view가 bar/line이고 chart_spec/rows가 유효할 때만 chart로 노출한다.
// y가 array면 첫 값을 사용해 단일 series로 좁힌다 (사용자 명시 1차 정책).
const mapChart = (dto: ComposerDisplayDto | undefined): ChatChart | undefined => {
  if (!dto) return undefined;
  const view = dto.recommended_view;
  const spec = dto.chart_spec;
  if (view !== "bar" && view !== "line") return undefined;
  if (!spec || (spec.kind !== "bar" && spec.kind !== "line")) return undefined;
  const rows = dto.rows ?? [];
  if (rows.length === 0) return undefined;
  const x = spec.x;
  const yKey = Array.isArray(spec.y) ? spec.y[0] : spec.y;
  if (!x || !yKey) return undefined;
  return {
    kind: spec.kind,
    x,
    y: yKey,
    title: dto.title ?? undefined,
    rows,
  };
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
  const assistantMessage: ChatMessage | undefined =
    content !== undefined && dto.assistant_message
      ? {
          id: dto.assistant_message.message_id,
          role: "assistant",
          content,
          createdAt: dto.assistant_message.created_at,
          display: mapDisplay(display),
          chart: mapChart(display),
          warnings: display?.warnings?.length ? display.warnings : undefined,
          taxonomyStatus: mapTaxonomyStatus(dto.result?.taxonomy_check),
          plan: mapPlan(dto.result?.plan),
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
const mapStoredMessage = (dto: AnalysisMessageDto): ChatMessage => ({
  id: dto.message_id,
  role: dto.role,
  content: dto.content,
  createdAt: dto.created_at,
  display: mapDisplay(dto.display),
  chart: mapChart(dto.display),
  warnings: dto.display?.warnings?.length ? dto.display.warnings : undefined,
});

export const mapThreadDetail = (dto: AnalysisThreadDetailDto): ChatThreadDetail => ({
  id: dto.thread_id,
  messages: dto.messages.map(mapStoredMessage),
});
