// 보고서 에디터의 기초분석 섹션(section 블록) 본문 — 데이터셋 "기초분석보고서" 탭과
// 동일하게 보이도록 공용 렌더러(BasicReportLayout)에 위임한다.
import BasicReportLayout from "@/features/versions/components/BasicReportLayout";
import type { ReportRow } from "@/features/versions/models/basicReport";
import type { TemplateRow } from "../models/editor";

export default function TemplateSection({ rows }: { rows: TemplateRow[] }) {
  // TemplateRow와 basicReport ReportRow는 구조가 동일(panels: view/width/title/value_format/data).
  return <BasicReportLayout layout={rows as unknown as ReportRow[]} />;
}
