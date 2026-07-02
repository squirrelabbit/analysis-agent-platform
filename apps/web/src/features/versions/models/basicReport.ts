// 기초분석보고서 탭 — 백엔드 GET .../basic_analysis 응답 계약.
// 블록 구조·값 표현(format)·배치(layout/view/width)는 모두 서버(템플릿 config)가
// 결정한다. 프론트는 이 타입대로 렌더만 하며 값을 재계산하지 않는다.
// 계약 원본: docs/api/report_basic_template.sample.md

export type PanelView =
  | "stat_grid"
  | "bar"
  | "doughnut"
  | "table"
  | "stacked_bar"
  | "rank"
  | "period_timeline"
  | "tag_list"
  | "definition_list";

export type ValueFormat = "count" | "percent" | "ratio" | "number" | "code" | "text";

// distribution (bar / doughnut / table)
export interface DistributionItem {
  key: string;
  label: string;
  count: number;
  percent: number;
}
export interface DistributionData {
  total: number;
  items: DistributionItem[];
}

// stacked_bar
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
export interface StackedData {
  categories: StackedCategory[];
  series: StackedSeries[];
}

// rank
export interface RankItem {
  rank: number;
  label: string;
  value: number;
}
export interface RankData {
  items: RankItem[];
}

// stat_grid
export interface StatItem {
  key: string;
  label: string;
  value: unknown;
  format?: ValueFormat;
  unit?: string;
  sub?: unknown;
}
export interface StatGridData {
  items: StatItem[];
}

// period_timeline (#31 분석 기간 — 연도별 대상기간·축제기간·기준/비교)
export interface PeriodTimelineRow {
  year: number;
  role: "base" | "compare"; // 기준 / 비교 연도
  role_label: string; // "기준 연도" / "비교 연도"
  target_start: string; // 대상기간 시작 YYYY-MM-DD
  target_end: string; // 대상기간 종료 YYYY-MM-DD
  target_days: number; // 대상기간 총 일수(양끝 포함)
  festival_start: string; // 축제기간 시작 YYYY-MM-DD
  festival_end: string; // 축제기간 종료 YYYY-MM-DD
}
export interface PeriodTimelineData {
  rows: PeriodTimelineRow[];
}

// tag_list (#31 수집 채널/키워드)
export interface TagListData {
  items: string[];
}

// definition_list (#31 유형 정의)
export interface DefinitionItem {
  term: string;
  description: string;
}
export interface DefinitionListData {
  items: DefinitionItem[];
}

export interface ReportPanel {
  view: PanelView;
  width: string; // "full" | "3/4" | "2/3" | "1/2" | "1/3" | "1/4"
  value_format?: ValueFormat;
  title?: string;
  data:
    | DistributionData
    | StackedData
    | RankData
    | StatGridData
    | PeriodTimelineData
    | TagListData
    | DefinitionListData;
  source?: Record<string, unknown>;
}

export interface ReportRow {
  panels: ReportPanel[];
}

export interface ReportBlock {
  block_id?: string;
  section_id: string;
  title?: string;
  unit_basis?: string;
  // 최신년도만 집계된 섹션 표시(예: "2025년 기준"). 개요 섹션·날짜 없는 경우엔 없음.
  scope_label?: string;
  layout: ReportRow[];
}

export interface ReportMissingSection {
  section_id: string;
  reason: string;
}

export interface BasicAnalysisResponse {
  template_id: string;
  dataset_version_id: string;
  title: string;
  blocks: ReportBlock[];
  included_sections: string[];
  missing_sections: ReportMissingSection[];
}
