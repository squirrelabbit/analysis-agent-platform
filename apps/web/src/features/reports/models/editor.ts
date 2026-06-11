// 분석 보고서 에디터 뷰모델.
// 보관함 항목(LibraryItem)·블록(ReportBlock)·캔버스 상태(ReportState).
// 결과 렌더는 채팅 결과 뷰 카탈로그를 재사용한다 — LibraryItem.result(ReportResult)로 보관.
// 실데이터(saved_results)는 models/library.ts의 어댑터가 LibraryItem으로 변환해 공급한다.
import type { ReportResult } from "./result";

export type LibType = "chart" | "table" | "text";

export interface LibraryItem {
  id: string;
  type: LibType;
  title: string;
  sub: string;
  /** 출처 분석 질문(원 질문) */
  question: string;
  /** 채팅과 동일하게 렌더하기 위한 결과 도메인(chart/metric/evidence/display/plan). */
  result: ReportResult;
}

// ── 보고서 블록 상태 ──
export interface BlockOpts {
  /** 원 질문 칩 표시 */
  q: boolean;
  /** 상세 데이터 폴드 표시 */
  detail: boolean;
  /** 분석 계획 폴드 표시 */
  plan: boolean;
}

export interface ReportBlock {
  uid: string;
  libId: string;
  /** 사용자 지정 표시 제목(null이면 라이브러리 원제목) */
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
   * true면 새 줄에서 시작(한 줄 차지). false면 앞 블록과 같은 줄에 이어 배치(나란히).
   * 자동 packing은 하지 않으며, 옆에 드롭할 때만 false로 설정된다.
   */
  newRow: boolean;
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
}

export const LIB_TYPE_LABEL: Record<LibType, string> = {
  chart: "차트",
  table: "표",
  text: "원문",
};

// 보고서 기본(빈) 상태 — 초기화 시 복원. 블록은 보관함에서 추가한다.
export const DEFAULT_STATE = (): ReportState => ({
  title: "분석 보고서",
  mode: "edit",
  selected: null,
  blocks: [],
});
