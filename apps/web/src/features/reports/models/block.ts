// 보고서 블록 정규화 — 서버가 돌려준 raw 블록(분석 결과 item / 기본 템플릿 섹션)이나
// 에디터가 이미 저장한 editor 블록을 모두 에디터 ReportBlock으로 변환한다(멱등).
//
// 저장(PUT) 시에는 에디터 ReportBlock을 그대로 직렬화해 보낸다(블록 contract는 프론트 소유).
// 따라서 normalizeBlock은 다음 셋을 모두 받아야 한다:
//   1) 에디터 블록(kind 보유) — 재로드/저장 round-trip
//   2) 분석 결과 item 블록(type=analysis_result / run_id / display 보유)
//   3) 기본 템플릿 섹션 블록(section_id / layout 보유)
import { createClientId } from "@/shared/utils/id";
import type { AnalysisPlanDto, ComposerDisplayDto } from "@/features/chats/models";
import { projectResult } from "./result";
import type {
  BlockOpts,
  ReportBlock,
  ResultSnapshot,
  SectionSnapshot,
  TemplateRow,
} from "./editor";

type Raw = Record<string, unknown>;

const asRecord = (v: unknown): Raw | undefined =>
  v && typeof v === "object" && !Array.isArray(v) ? (v as Raw) : undefined;

const str = (v: unknown): string | undefined =>
  typeof v === "string" ? v : undefined;

const num = (v: unknown): number | undefined =>
  typeof v === "number" && Number.isFinite(v) ? v : undefined;

const bool = (v: unknown, fallback: boolean): boolean =>
  typeof v === "boolean" ? v : fallback;

// 분석 결과 기본 제목 — title → display.title → question → assistant_content 순.
function deriveResultTitle(
  rawTitle: string | undefined,
  display: ComposerDisplayDto | undefined,
  question: string,
  assistantContent: string,
): string {
  const fromTitle = rawTitle?.trim();
  if (fromTitle) return fromTitle;
  const fromDisplay = display?.title?.trim();
  if (fromDisplay) return fromDisplay;
  const fromQ = question.trim();
  if (fromQ) return fromQ;
  const fromA = assistantContent.trim();
  if (fromA) return fromA.length > 40 ? `${fromA.slice(0, 40)}…` : fromA;
  return "분석 결과";
}

// 상세 데이터(접이식 표) 보유 여부 — 메인이 표가 아닌데 display 표가 있으면 true.
function resultHasDetail(snapshot: ResultSnapshot): boolean {
  const r = projectResult(snapshot);
  return (!!r.metric || !!r.evidence || !!r.chart) && !!r.display;
}

function resultOptsFrom(options: Raw | undefined, snapshot: ResultSnapshot): BlockOpts {
  return {
    q: bool(options?.show_question, true),
    detail: bool(options?.show_detail, resultHasDetail(snapshot)),
    plan: bool(options?.show_plan, false),
  };
}

interface Layout {
  span: number;
  height: number | null;
  newRow: boolean;
}

function layoutFrom(
  layout: Raw | undefined,
  defaults: Layout,
): Layout {
  return {
    span: num(layout?.span) ?? defaults.span,
    height: num(layout?.height) ?? defaults.height,
    newRow: bool(layout?.new_row, defaults.newRow),
  };
}

// ── 에디터 블록(이미 정규화된 형태) 보정 — 누락 필드 기본값 채움 ──
function coerceEditorBlock(raw: Raw): ReportBlock {
  const kind = raw.kind === "section" ? "section" : "result";
  const opts = asRecord(raw.opts);
  const base: ReportBlock = {
    uid: str(raw.uid) || createClientId(),
    kind,
    title: typeof raw.title === "string" ? raw.title : null,
    interp: str(raw.interp) ?? "",
    opts: {
      q: bool(opts?.q, kind === "result"),
      detail: bool(opts?.detail, false),
      plan: bool(opts?.plan, false),
    },
    span: num(raw.span) ?? 12,
    height: num(raw.height) ?? null,
    newRow: bool(raw.newRow, true),
  };
  if (kind === "section") {
    base.section = (raw.section as SectionSnapshot) ?? {
      sectionId: "",
      defaultTitle: "",
      rows: [],
    };
  } else {
    base.result = (raw.result as ResultSnapshot) ?? {
      question: "",
      assistantContent: "",
      defaultTitle: "분석 결과",
    };
  }
  return base;
}

// ── 분석 결과 item 블록 → 에디터 블록 ──
function fromAnalysisResult(raw: Raw): ReportBlock {
  const question = str(raw.question) ?? "";
  const assistantContent = str(raw.assistant_content) ?? "";
  const display = asRecord(raw.display) as ComposerDisplayDto | undefined;
  const plan = asRecord(raw.plan) as AnalysisPlanDto | undefined;
  const snapshot: ResultSnapshot = {
    runId: str(raw.run_id),
    threadId: str(raw.thread_id),
    question,
    assistantContent,
    defaultTitle: deriveResultTitle(str(raw.title), display, question, assistantContent),
    display,
    plan,
  };
  const layout = layoutFrom(asRecord(raw.layout), {
    span: 12,
    height: null,
    newRow: true,
  });
  return {
    uid: str(raw.uid) || createClientId(),
    kind: "result",
    title: null,
    interp: str(raw.interp) ?? "",
    opts: resultOptsFrom(asRecord(raw.options), snapshot),
    span: layout.span,
    height: layout.height,
    newRow: layout.newRow,
    result: snapshot,
  };
}

// ── 기본 템플릿 섹션 블록 → 에디터 블록(섹션 1개 = 전체폭 카드) ──
function fromTemplateSection(raw: Raw): ReportBlock {
  const rows = Array.isArray(raw.layout) ? (raw.layout as TemplateRow[]) : [];
  const section: SectionSnapshot = {
    sectionId: str(raw.section_id) ?? "",
    defaultTitle: str(raw.title) ?? "",
    unitBasis: str(raw.unit_basis),
    scopeLabel: str(raw.scope_label),
    rows,
  };
  return {
    uid: str(raw.block_id) || createClientId(),
    kind: "section",
    title: null,
    interp: "",
    opts: { q: false, detail: false, plan: false },
    span: 12,
    height: null,
    newRow: true,
    section,
  };
}

// raw 블록 1개 → 에디터 ReportBlock(변환 불가하면 null).
export function normalizeBlock(input: unknown): ReportBlock | null {
  const raw = asRecord(input);
  if (!raw) return null;
  // 1) 이미 에디터 블록(kind 보유)
  if (raw.kind === "result" || raw.kind === "section") return coerceEditorBlock(raw);
  // 2) 기본 템플릿 섹션(section_id / layout 배열)
  if (typeof raw.section_id === "string" || Array.isArray(raw.layout))
    return fromTemplateSection(raw);
  // 3) 분석 결과 item(type=analysis_result / run_id / display)
  if (
    raw.type === "analysis_result" ||
    typeof raw.run_id === "string" ||
    "display" in raw ||
    "assistant_content" in raw
  )
    return fromAnalysisResult(raw);
  return null;
}

export function normalizeBlocks(input: unknown[]): ReportBlock[] {
  return (input ?? [])
    .map(normalizeBlock)
    .filter((b): b is ReportBlock => b !== null);
}
