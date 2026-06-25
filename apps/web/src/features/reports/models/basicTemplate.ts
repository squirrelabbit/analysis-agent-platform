// 데이터 기초 분석 보고서 "기본 템플릿" 계약 타입.
// 원본 계약: apps/web/src/assets/template/report_basic_template.sample.md
//
//   report → blocks[] (흰 카드)
//            └─ layout[] (행 row)
//               └─ panels[] = { view, width, title?, value_format?, data }  ← 자급자족
//
// 패널이 data를 자체 보유하므로 한 카드에 서로 다른 data/view를 섞을 수 있다.
// 백엔드 POST /projects/{pid}/reports/from_template 응답이 이 모양을 따른다(예정).

export type PanelView =
  | "stat_grid"
  | "stat_cards"
  | "bar"
  | "doughnut"
  | "table"
  | "stacked_bar"
  | "rank"
  | "text";

// 값 표현 태그(템플릿이 결정).
//   count → "5,131"(+unit)  percent → "57.4%"  ratio → "0.57"
//   number → "8940"         code → 모노         text → 그대로(+sub)
export type ValueFormat =
  | "count"
  | "percent"
  | "ratio"
  | "number"
  | "code"
  | "text";

// width ∈ row 분할. 한 행을 넘으면 다음 줄로 내려간다.
export type PanelWidth = "full" | "3/4" | "2/3" | "1/2" | "1/3" | "1/4";

// stat_grid(컴팩트 셀) / stat_cards(강조 카드) 공용 항목.
export interface StatItem {
  key: string;
  label: string;
  value: string | number;
  format: ValueFormat;
  unit?: string;
  sub?: string;
  // stat_cards 전용(선택): 우상단 뱃지 텍스트 + 강조 톤.
  badge?: string;
  accent?: "primary" | "muted";
}
export interface StatGridData {
  items: StatItem[];
}

// bar / doughnut / (분포)table — count·percent 둘 다 실어 준다(주 축=value_format).
export interface DistItem {
  key: string;
  label: string;
  count: number;
  percent: number;
}
export interface DistData {
  total: number;
  items: DistItem[];
}

// table은 분포 data(DistData) 또는 일반 표(columns/rows) 둘 다 받는다.
export interface ColumnsTableData {
  columns: string[];
  rows: (string | number)[][];
}
export type TableData = DistData | ColumnsTableData;

// stacked_bar — 유형별 감성 100% 누적.
export interface StackedCategory {
  key: string;
  label: string;
  total: number;
}
export interface StackedSeries {
  key: string;
  label: string;
  counts: number[];
  percents: number[];
}
export interface StackedBarData {
  categories: StackedCategory[];
  series: StackedSeries[];
}

// rank — 순위 1열.
export interface RankItem {
  rank: number;
  label: string;
  value: number;
}
export interface RankData {
  items: RankItem[];
}

// text — 설명.
export interface TextData {
  markdown: string;
}

export type PanelData =
  | StatGridData
  | DistData
  | TableData
  | StackedBarData
  | RankData
  | TextData;

export interface BasicPanel {
  view: PanelView;
  width: PanelWidth;
  title?: string;
  value_format?: ValueFormat;
  data: PanelData;
}

export interface BasicRow {
  panels: BasicPanel[];
}

export interface BasicBlock {
  block_id: string;
  section_id: string;
  title: string;
  /** 분석 단위 뱃지(문서/절). */
  unit_basis?: "doc" | "clause";
  /** true면 외곽 흰 카드(테두리/그림자) 없이 콘텐츠만 렌더 — 자체 카드를 가진 패널용. */
  bare?: boolean;
  layout: BasicRow[];
}

export interface BasicReport {
  report_id?: string;
  project_id?: string;
  dataset_version_id?: string;
  title: string;
  blocks: BasicBlock[];
  created_at?: string;
  updated_at?: string;
}

// from_template 응답 envelope.
export interface BasicReportResponse {
  report: BasicReport;
  included_sections?: string[];
  missing_sections?: string[];
}
