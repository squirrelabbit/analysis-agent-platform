// 분석 보고서 에디터 뷰모델.
// 보고서 블록은 self-contained snapshot이다 — 채팅 분석 결과(result) 또는 기본 템플릿
// 섹션(section) 둘 중 하나를 들고 있고, 블록 자체에 렌더에 필요한 데이터를 모두 보존한다.
// (옛 saved_results 보관함 + libId 참조 모델은 제거됐다 — 백엔드 item/from_template
//  엔드포인트가 블록에 스냅샷을 박제해 돌려준다.)
import type { AnalysisPlanDto, ComposerDisplayDto } from "@/features/chats/models";

// ── 블록 콘텐츠 스냅샷 ──
export type BlockKind = "result" | "section";

// 채팅 분석 결과 스냅샷(item 블록). display/plan은 채팅 composer와 동일 shape.
export interface ResultSnapshot {
  runId?: string;
  threadId?: string;
  question: string;
  assistantContent: string;
  /** 결과 기본 제목(block.title이 null이면 이 값 사용). */
  defaultTitle: string;
  display?: ComposerDisplayDto;
  plan?: AnalysisPlanDto;
}

// 기본 템플릿 패널(자급자족 — view별로 data 모양이 다르다).
// 계약: docs/api/report_basic_template.sample.md.
export type TemplateView =
  | "stat_grid"
  | "bar"
  | "doughnut"
  | "table"
  | "stacked_bar"
  | "rank"
  | "text";

export interface TemplatePanel {
  view: TemplateView | string;
  /** "full" | "3/4" | "2/3" | "1/2" | "1/3" | "1/4" */
  width: string;
  title?: string;
  /** count | percent | ratio | number | code | text (주 축 값 표현). */
  value_format?: string;
  data: Record<string, unknown>;
}

export interface TemplateRow {
  panels: TemplatePanel[];
}

// 기본 템플릿 섹션 스냅샷(흰 카드 1장 = 한 블록).
export interface SectionSnapshot {
  sectionId: string;
  defaultTitle: string;
  /** "doc" | "clause" 등 집계 단위(메타). */
  unitBasis?: string;
  /** "2025년 기준" 등 — 제목 옆 배지. */
  scopeLabel?: string;
  rows: TemplateRow[];
}

// ── 보고서 블록 상태 ──
export interface BlockOpts {
  /** 원 질문 칩 표시 (result 블록 전용) */
  q: boolean;
  /** 상세 데이터 폴드 표시 (result 블록 전용) */
  detail: boolean;
  /** 분석 계획 폴드 표시 (result 블록 전용) */
  plan: boolean;
}

export interface ReportBlock {
  uid: string;
  /** 콘텐츠 종류 — result(분석 결과) | section(기본 템플릿 섹션). */
  kind: BlockKind;
  /** 사용자 지정 표시 제목(null이면 스냅샷 기본 제목) */
  title: string | null;
  /** 해석 문구 */
  interp: string;
  opts: BlockOpts;
  /**
   * 12컬럼 그리드 기준 차지 컬럼 수(span). 12=전체, 6=½, 4=⅓, 8=⅔, 3=¼, 9=¾.
   * 그리드라 같은 span끼리 너비가 정확히 같고 gap도 자동 정렬된다.
   */
  span: number;
  /**
   * 카드 최소 높이(px). null이면 콘텐츠 높이(자동). 하단 모서리 드래그로 늘린다.
   * minHeight 기준이라 콘텐츠보다 짧게는 줄지 않아 차트 등이 잘리지 않는다.
   */
  height: number | null;
  /**
   * true면 새 줄에서 시작(한 줄 차지). false면 앞 블록과 같은 줄에 이어 배치(나란히).
   */
  newRow: boolean;
  /** kind==="result" */
  result?: ResultSnapshot;
  /** kind==="section" */
  section?: SectionSnapshot;
}

// 보고서 캔버스 그리드 컬럼 수.
export const GRID_COLS = 12;
// 리사이즈가 스냅되는 span 후보(¼/⅓/½/⅔/¾/전체).
export const BLOCK_SPANS = [3, 4, 6, 8, 9, 12];
// span → 분수 라벨.
export const BLOCK_SPAN_LABEL: Record<number, string> = {
  3: "¼",
  4: "⅓",
  6: "½",
  8: "⅔",
  9: "¾",
  12: "전체",
};
export const spanLabel = (span: number): string =>
  BLOCK_SPAN_LABEL[span] ?? `${span}/${GRID_COLS}`;
// 가장 가까운 후보 span으로 스냅(동률이면 더 큰 쪽).
export const snapSpan = (raw: number): number => {
  const s = Math.min(GRID_COLS, Math.max(BLOCK_SPANS[0], Math.round(raw)));
  let best = BLOCK_SPANS[0];
  let bestDist = Infinity;
  for (const cand of BLOCK_SPANS) {
    const d = Math.abs(cand - s);
    if (d < bestDist || (d === bestDist && cand > best)) {
      bestDist = d;
      best = cand;
    }
  }
  return best;
};

export type ReportMode = "edit" | "preview";

export interface ReportState {
  title: string;
  mode: ReportMode;
  blocks: ReportBlock[];
  /** 선택된 블록 uid */
  selected: string | null;
  /**
   * 현재 state가 어떤 보고서(reportId)로 hydrate됐는지. null이면 아직 미hydrate(DEFAULT).
   * 자동저장 가드가 이 값으로 "현재 state가 이 보고서 것인지"를 state와 원자적으로 판단한다.
   */
  hydratedId: string | null;
}

// 보고서 기본(빈) 상태 — 초기화 시 복원. 블록은 채팅 분석 결과/템플릿에서 들어온다.
export const DEFAULT_STATE = (): ReportState => ({
  title: "분석 보고서",
  mode: "edit",
  selected: null,
  blocks: [],
  hydratedId: null,
});
