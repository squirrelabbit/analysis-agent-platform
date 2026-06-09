// 분석 보고서 페이지 샘플 모델.
// 분석 채팅에서 생성된 결과(차트/표)를 보고서 항목으로 모아 선택·다운로드한다.
// NOTE: 현재는 디자인 샘플용 mock 데이터. 실제 연동 시 채팅 결과 저장/조회 API
//       계약(결과 id, 타입, 원본 plan step, 채팅·메시지 참조 등)이 필요하다.

export type ResultKind = "chart" | "table";
export type ResultViz = "donut" | "bars" | "stack" | "table";

export interface Segment {
  label: string;
  value: number; // 퍼센트(0~100)
  color: string;
}

export interface TablePreview {
  head: string[];
  rows: string[][];
}

export interface ReportResult {
  id: string;
  kind: ResultKind;
  viz: ResultViz;
  title: string;
  /** 출처 분석 채팅 질문 */
  chat: string;
  time: string;
  /** "3,647 문장" 같은 규모 라벨 */
  rows: string;
  /** 날짜 그룹 라벨 ("오늘" 등) */
  group: string;
  segments?: Segment[]; // donut / stack
  bars?: { label: string; value: number }[];
  table?: TablePreview;
}

export type ReportFormat = "pdf" | "html" | "xlsx";

// 감성 색 — 앱 공통 팔레트와 일치(positive/neutral/negative).
export const SENT = {
  pos: "#10b981",
  neu: "#c9cad2",
  neg: "#ef4444",
} as const;

export const MOCK_RESULTS: ReportResult[] = [
  {
    id: "r1",
    kind: "chart",
    viz: "donut",
    title: "Aspect별 감성 분포",
    chat: "공연 만족도 어땠어?",
    time: "14:22",
    rows: "3,647 문장",
    group: "오늘",
    segments: [
      { label: "긍정", value: 62, color: SENT.pos },
      { label: "중립", value: 31, color: SENT.neu },
      { label: "부정", value: 7, color: SENT.neg },
    ],
  },
  {
    id: "r2",
    kind: "table",
    viz: "table",
    title: "부정 반응 문장 TOP 20",
    chat: "불만 사항만 모아줘",
    time: "14:05",
    rows: "20 행",
    group: "오늘",
    table: {
      head: ["문장", "주제", "감성"],
      rows: [
        ["주차장이 너무 부족…", "교통", "부정"],
        ["배차 간격이 길어…", "교통", "부정"],
        ["화장실 줄이 길…", "편의", "부정"],
      ],
    },
  },
  {
    id: "r3",
    kind: "chart",
    viz: "bars",
    title: "Aspect 언급량 TOP 8",
    chat: "주제별로 몇 건씩이야?",
    time: "13:48",
    rows: "8 항목",
    group: "오늘",
    bars: [
      { label: "공연/프로그램", value: 983 },
      { label: "체험/부스", value: 619 },
      { label: "분위기/경관", value: 580 },
      { label: "음식", value: 382 },
    ],
  },
  {
    id: "r4",
    kind: "chart",
    viz: "stack",
    title: "전체 긍·부정 비율",
    chat: "전체 감성 비율 알려줘",
    time: "11:30",
    rows: "3,647 문장",
    group: "오늘",
    segments: [
      { label: "긍정", value: 62, color: SENT.pos },
      { label: "중립", value: 31, color: SENT.neu },
      { label: "부정", value: 7, color: SENT.neg },
    ],
  },
  {
    id: "r5",
    kind: "table",
    viz: "table",
    title: "진성/비진성 판별 요약",
    chat: "진짜 후기 비율은?",
    time: "10:12",
    rows: "2,121 문서",
    group: "오늘",
    table: {
      head: ["판별", "건수", "비율"],
      rows: [
        ["진성", "395 건", "18.6%"],
        ["비진성", "1,718 건", "81.0%"],
        ["불확실", "8 건", "0.4%"],
      ],
    },
  },
  {
    id: "r6",
    kind: "chart",
    viz: "donut",
    title: "진성 문서 비율",
    chat: "진짜 후기 비율은?",
    time: "10:11",
    rows: "2,121 문서",
    group: "오늘",
    segments: [
      { label: "진성", value: 18.6, color: SENT.pos },
      { label: "비진성", value: 81, color: SENT.neg },
      { label: "불확실", value: 0.4, color: SENT.neu },
    ],
  },
];
