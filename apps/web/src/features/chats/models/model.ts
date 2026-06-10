import type { ChatRole } from "./dto";
import type { ColumnFormat } from "./format";

export interface ChatTableDisplay {
  type: "table";
  title?: string;
  columns: string[];
  rows: Record<string, unknown>[];
  // silverone 2026-06-09 — compare 결과 컬럼별 표시 포맷/라벨(백엔드 contract).
  // 없으면 raw 렌더(기존 동작 유지).
  columnFormats?: Record<string, ColumnFormat>;
  columnLabels?: Record<string, string>;
}

export type ChatDisplay = ChatTableDisplay;

export interface ChatPlanStep {
  id: string;
  skill: string;
  params: Record<string, unknown>;
  label?: string;
  expression?: string;
}

export interface ChatPlan {
  version: string;
  steps: ChatPlanStep[];
}

export interface ChatChart {
  kind: "bar" | "line" | "diverging_bar";
  x: string;
  // 1차 구현: 단일 numeric series 우선. 응답이 array면 매퍼에서 첫 값으로 좁힌다.
  y: string;
  title?: string;
  rows: Record<string, unknown>[];
  // silverone 2026-06-09 — y 컬럼의 표시 포맷(백엔드 contract). point/percent면
  // 0~1 비율을 %p/% 스케일로 올려 그리고 단위를 표시한다.
  yFormat?: ColumnFormat;
  yLabel?: string;
  // silverone 2026-06-09 — diverging_bar 단위(건/%p/%). 백엔드 chart_spec.unit.
  unit?: string;
  // silverone 2026-06-09 — distribution(비중) 막대 라벨 보조 건수 컬럼.
  // y=비중(%)일 때 막대 라벨에 "(N건)"으로 붙인다. 백엔드 chart_spec.count_col.
  countKey?: string;
  // line — 기준일 기준선 (YYYY-MM-DD). 백엔드 chart_spec.event_date.
  eventDate?: string;
  // 기준선 라벨(백엔드 chart_spec.event_label). 없으면 뷰가 "기준일" fallback.
  eventLabel?: string;
}

export type TaxonomyStatus =
  | "ok"
  | "legacy_missing"
  | "hash_mismatch"
  | "id_mismatch";

// silverone 2026-06-09 — total 기간 비교(1행) metric card.
export interface ChatMetric {
  aValue: number | null;
  bValue: number | null;
  deltaValue: number | null;
  deltaRate: number | null; // percent
  unit: string;
}

// silverone 2026-06-09 — 원문 샘플(sample_rows) evidence card.
export interface ChatEvidenceChip {
  key: string;
  value: string;
}
export interface ChatEvidenceItem {
  text: string;
  sentiment?: string;
  chips: ChatEvidenceChip[];
  id?: string;
}
export interface ChatEvidence {
  items: ChatEvidenceItem[];
  total: number;
}

// 메인 렌더 view. 백엔드가 새 타입(stacked_bar 등)을 추가할 수 있어
// 알려진 값만 좁히고 그 외는 "unknown"으로 떨어뜨려 table fallback한다.
export type RecommendedView =
  | "table"
  | "bar"
  | "diverging_bar"
  | "line"
  | "metric"
  | "evidence"
  | "unknown";

export type RunStatus = "running" | "completed" | "failed";

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: string;
  display?: ChatDisplay;
  plan?: ChatPlan;
  chart?: ChatChart;
  metric?: ChatMetric;
  evidence?: ChatEvidence;
  warnings?: string[];
  taxonomyStatus?: TaxonomyStatus;
  // 백엔드가 chart를 추천했지만 유효 데이터 부족 등으로 매퍼가 chart를
  // 생성하지 못한 경우에 fallback 안내를 띄우기 위한 플래그.
  chartFallbackReason?: "insufficient_data";
  recommendedView?: RecommendedView;
  // run 정보 — POST 응답에만 존재. 이력 메시지(thread detail)에는 없어
  // 표시되지 않는다 (사용자 정책).
  runStatus?: RunStatus;
  runError?: string;
}

export interface ChatThread {
  id: string;
  title: string;
  lastMessage: string;
  messageCount: number;
  updatedAt: string;
}

export interface ChatThreadDetail {
  id: string;
  messages: ChatMessage[];
}

export interface AnalyzeResult {
  threadId: string;
  userMessage?: ChatMessage;
  assistantMessage?: ChatMessage;
  errorMessage?: string | null;
}
