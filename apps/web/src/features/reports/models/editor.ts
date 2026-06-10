// 분석 보고서 에디터 모델.
// 분석 채팅에서 저장된 결과(차트/표/원문)를 "보관함"에서 골라 보고서 블록으로 구성한다.
// NOTE: 현재 LIBRARY는 디자인 샘플용 mock(festival 데이터). 실제 연동 시 채팅 결과
//       저장/조회 API 계약(결과 id·타입·viz·원본 plan step·채팅 참조 등)이 필요하다.
//       기존 list형 디자인 모델은 models/model.ts에 유지(Sidebar 배지가 참조).

// ── viz 색 (앱 공통 팔레트와 일치) ──
export const VIZ_COLOR = {
  pos: "#10b981", // emerald-500
  neu: "#a1a1aa", // zinc-400
  neg: "#ef4444", // red-500
  primary: "#7c3aed", // violet-600
  blue: "#3b82f6", // blue-500
} as const;

// ── viz 종류별 데이터 타입 ──
export type VizKey =
  | "bars"
  | "diverge"
  | "donut"
  | "line"
  | "metric"
  | "evidence"
  | "grid";

export interface BarsData {
  rows: { k: string; v: number }[];
}
export interface DivergeData {
  unit?: string;
  rows: { k: string; v: number }[];
}
export interface DonutData {
  rows: { k: string; pct: number; n: string; color: string }[];
}
export interface LinePoint {
  x: string;
  v: number;
  mark?: boolean;
}
export interface LineData {
  max: number;
  refIdx?: number;
  refLabel?: string;
  pts: LinePoint[];
}
export interface MetricData {
  beforeK: string;
  afterK: string;
  before: number;
  after: number;
  unit: string;
  delta: string;
}
export type Sentiment = "pos" | "neu" | "neg";
export interface EvidenceData {
  rows: { q: string; aspect: string; s: Sentiment; doc: string }[];
}
export interface GridData {
  cols: string[];
  rows: string[][];
}

export type VizData =
  | BarsData
  | DivergeData
  | DonutData
  | LineData
  | MetricData
  | EvidenceData
  | GridData;

export type LibType = "chart" | "table" | "text";

export interface LibraryItem {
  id: string;
  type: LibType;
  title: string;
  sub: string;
  /** 출처 분석 질문(원 질문) */
  question: string;
  viz: VizKey;
  data: VizData;
  /** 펼침 "상세 데이터" 표 (없으면 null) */
  detail: GridData | null;
  /** 펼침 "분석 계획" 단계 */
  plan: string[];
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

// ── 저장된 결과 보관함 (mock) ──
export const LIBRARY: LibraryItem[] = [
  {
    id: "agg",
    type: "chart",
    title: "Aspect별 부정 언급 건수",
    sub: "상위 5개 · 내림차순",
    question: "부정 후기가 많은 aspect TOP 5",
    viz: "bars",
    data: {
      rows: [
        { k: "음식", v: 312 },
        { k: "교통/접근성", v: 244 },
        { k: "혼잡도", v: 190 },
        { k: "편의시설", v: 131 },
        { k: "운영/서비스", v: 88 },
      ],
    },
    detail: {
      cols: ["Aspect", "부정 건수"],
      rows: [
        ["음식", "312"],
        ["교통/접근성", "244"],
        ["혼잡도", "190"],
        ["편의시설", "131"],
        ["운영/서비스", "88"],
      ],
    },
    plan: [
      "전체 후기에서 부정 감성 문장 필터",
      "aspect taxonomy로 분류·집계",
      "건수 내림차순 정렬 후 상위 5개 추출",
    ],
  },
  {
    id: "donut",
    type: "chart",
    title: "감성 구성비",
    sub: "전체 2,121건 기준",
    question: "전체 후기의 감성 구성비 보여줘",
    viz: "donut",
    data: {
      rows: [
        { k: "긍정", pct: 56, n: "1,188건", color: VIZ_COLOR.pos },
        { k: "중립", pct: 25, n: "530건", color: VIZ_COLOR.neu },
        { k: "부정", pct: 19, n: "403건", color: VIZ_COLOR.neg },
      ],
    },
    detail: {
      cols: ["감성", "비율", "건수"],
      rows: [
        ["긍정", "56%", "1,188"],
        ["중립", "25%", "530"],
        ["부정", "19%", "403"],
      ],
    },
    plan: ["전체 후기 감성 라벨 집계", "비율(%) 산출 후 도넛 구성"],
  },
  {
    id: "line",
    type: "chart",
    title: "일자별 게시물 수",
    sub: "2025-08-12 ~ 08-18",
    question: "축제일 2025-08-15 전후 7일 게시물 수 추이",
    viz: "line",
    data: {
      max: 160,
      refIdx: 3,
      refLabel: "축제일",
      pts: [
        { x: "8/12", v: 42 },
        { x: "8/13", v: 58 },
        { x: "8/14", v: 96 },
        { x: "8/15", v: 152, mark: true },
        { x: "8/16", v: 110 },
        { x: "8/17", v: 74 },
        { x: "8/18", v: 51 },
      ],
    },
    detail: {
      cols: ["일자", "게시물 수"],
      rows: [
        ["8/12", "42"],
        ["8/13", "58"],
        ["8/14", "96"],
        ["8/15", "152"],
        ["8/16", "110"],
        ["8/17", "74"],
        ["8/18", "51"],
      ],
    },
    plan: [
      "created_at 기준 일자별 게시물 수 집계",
      "이벤트일(8/15) 전후 7일 윈도우 추출",
      "x축 오름차순 정렬",
    ],
  },
  {
    id: "diverge",
    type: "chart",
    title: "Aspect별 언급 비중 증감",
    sub: "축제 전 7일 → 후 7일 · 단위 %p",
    question: "축제 전후 aspect별 언급량 증감률",
    viz: "diverge",
    data: {
      unit: "",
      rows: [
        { k: "공연/프로그램", v: 29.1 },
        { k: "분위기/경관", v: 16.5 },
        { k: "체험/부스", v: 6.8 },
        { k: "교통/접근성", v: -11.7 },
        { k: "혼잡도", v: -18.4 },
      ],
    },
    detail: {
      cols: ["Aspect", "증감(%p)"],
      rows: [
        ["공연/프로그램", "+29.1"],
        ["분위기/경관", "+16.5"],
        ["체험/부스", "+6.8"],
        ["교통/접근성", "−11.7"],
        ["혼잡도", "−18.4"],
      ],
    },
    plan: [
      "전·후 기간별 aspect 언급 비중 계산",
      "비중 차이(delta %p) 산출",
      "0 기준 다이버징 정렬",
    ],
  },
  {
    id: "metric",
    type: "chart",
    title: "전체 게시물 수 · 전후 비교",
    sub: "각 7일 합계",
    question: "축제 전후 전체 게시물 수 비교",
    viz: "metric",
    data: {
      beforeK: "전 (8/8–8/14)",
      afterK: "후 (8/15–8/21)",
      before: 388,
      after: 512,
      unit: "건",
      delta: "+32.0%",
    },
    detail: {
      cols: ["기간", "게시물 수"],
      rows: [
        ["전 (8/8–8/14)", "388"],
        ["후 (8/15–8/21)", "512"],
        ["증감", "+124 (+32.0%)"],
      ],
    },
    plan: ["전·후 7일 게시물 수 각각 합계", "변화량·변화율 산출"],
  },
  {
    id: "table",
    type: "table",
    title: "Aspect별 감성 집계",
    sub: "상위 5개 · compact grid",
    question: "aspect별 긍·부정 건수 표로 보여줘",
    viz: "grid",
    data: {
      cols: ["Aspect", "긍정", "중립", "부정", "합계"],
      rows: [
        ["공연/프로그램", "612", "141", "47", "800"],
        ["음식", "203", "98", "312", "613"],
        ["분위기/경관", "388", "74", "29", "491"],
        ["교통/접근성", "52", "66", "244", "362"],
        ["혼잡도", "31", "58", "190", "279"],
      ],
    },
    detail: null,
    plan: ["aspect × 감성 교차 집계", "합계 컬럼 계산 후 정렬"],
  },
  {
    id: "text",
    type: "text",
    title: "음식 부정 문장 예시",
    sub: "대표 3개",
    question: "음식 관련 부정 문장 예시 보여줘",
    viz: "evidence",
    data: {
      rows: [
        {
          q: "먹거리 장터는 종류는 많은데 가격이 다른 축제보다 확실히 비쌌어요.",
          aspect: "음식",
          s: "neg",
          doc: "d1029f3b…",
        },
        {
          q: "인기 부스는 한 시간씩 줄 서야 해서 결국 못 먹고 돌아왔습니다.",
          aspect: "음식",
          s: "neg",
          doc: "a77c0e91…",
        },
        {
          q: "먹거리는 종류는 많은데 줄이 길고 자리가 부족해 서서 먹어야 했어요.",
          aspect: "음식",
          s: "neg",
          doc: "2272a91f…",
        },
      ],
    },
    detail: null,
    plan: ["음식 aspect + 부정 감성 문장 필터", "대표성 높은 원문 샘플링"],
  },
];

export const libById = (id: string): LibraryItem | undefined =>
  LIBRARY.find((l) => l.id === id);

export const LIB_TYPE_LABEL: Record<LibType, string> = {
  chart: "차트",
  table: "표",
  text: "원문",
};

export const SENT_LABEL: Record<Sentiment, string> = {
  pos: "긍정",
  neu: "중립",
  neg: "부정",
};

// 보고서 기본(예시) 상태 — 초기화 시 복원.
export const DEFAULT_STATE = (): ReportState => ({
  title: "2025 여름축제 후기 분석 보고서",
  mode: "edit",
  selected: null,
  blocks: [
    {
      uid: "b1",
      libId: "agg",
      title: null,
      interp:
        "부정 의견은 음식·교통 영역에 집중됐습니다. 특히 음식은 2위 대비 1.3배 많아 우선 개선 대상입니다.",
      opts: { q: true, detail: true, plan: false },
      span: 12,
      newRow: true,
    },
    {
      uid: "b2",
      libId: "metric",
      title: null,
      interp: "",
      opts: { q: true, detail: false, plan: false },
      span: 6,
      newRow: true,
    },
    {
      uid: "b3",
      libId: "donut",
      title: null,
      interp: "",
      opts: { q: true, detail: false, plan: false },
      span: 6,
      // b2와 같은 줄(나란히) — 명시적 side-by-side 예시.
      newRow: false,
    },
  ],
});
