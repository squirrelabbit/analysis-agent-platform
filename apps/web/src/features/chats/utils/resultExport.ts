// 결과 카드 액션 직렬화 유틸. (silverone 2026-06-11)
// 복사와 다운로드의 역할을 분리한다:
//   - 복사  = 문서/노션/Obsidian/메신저에 붙여넣기 좋은 Markdown(제목 + 요약 + 표)
//   - 다운로드 = 스프레드시트용 CSV 원자료
// display(표)가 있으면 그 컬럼/행을, 없고 chart만 있으면 chart의 x/y를 데이터로
// 사용한다. metric/evidence-only 결과는 표 데이터가 없어 텍스트(요약)만.
import type { ChatMessage } from "../models";
import { formatCellValue } from "../models/format";
import type { ColumnFormat } from "../models/format";

// 복사 표가 너무 길어지지 않도록 상위 N행만 담고 나머지는 "외 N건"으로 표기.
const COPY_MAX_ROWS = 50;

interface ResultTable {
  columns: string[];
  rows: Record<string, unknown>[];
  title?: string;
  labels: Record<string, string>; // 헤더 표시 라벨(있으면)
  formats: Record<string, ColumnFormat>; // 값 표시 포맷(%, %p, 정수)
}

// 결과에서 표 형태 데이터를 추출. 없으면 null.
function resultTable(message: ChatMessage): ResultTable | null {
  const d = message.display;
  if (d && d.columns.length > 0) {
    return {
      columns: d.columns,
      rows: d.rows,
      title: d.title,
      labels: d.columnLabels ?? {},
      formats: d.columnFormats ?? {},
    };
  }
  const c = message.chart;
  if (c && c.rows.length > 0) {
    return {
      columns: [c.x, c.y],
      rows: c.rows,
      title: c.title,
      labels: c.yLabel ? { [c.y]: c.yLabel } : {},
      formats: c.yFormat ? { [c.y]: c.yFormat } : {},
    };
  }
  return null;
}

function isNumericLike(v: unknown): boolean {
  if (typeof v === "number") return Number.isFinite(v);
  if (typeof v === "string" && v.trim() !== "") return Number.isFinite(Number(v));
  return false;
}

// "분석 결과 N건을 …로 정리했습니다" 류 generic narration은 복사에서 제외한다
// (정보 가치 없는 메타 문장). 의미 있는 요약(증감 설명 등)은 그대로 둔다.
function isGenericNarration(text: string): boolean {
  return /^분석\s*결과\s*\d+\s*건.*(정리했습니다|정리하였습니다)\.?$/.test(text.trim());
}

// 셀/헤더 안의 표 구분자·줄바꿈이 Markdown 표를 깨지 않도록 이스케이프.
function mdCell(s: string): string {
  return s.replace(/\|/g, "\\|").replace(/\r?\n/g, " ").trim();
}

// 클립보드 복사용 Markdown. 제목(###) + (의미 있는) 요약 + Markdown 표.
// 요약이 generic이거나 없으면 제목 + 표만. 표가 없으면 요약 텍스트만.
export function buildCopyText(message: ChatMessage): string {
  const table = resultTable(message);
  const blocks: string[] = [];

  const title = table?.title?.trim();
  if (title) blocks.push(`### ${title}`);

  const content = message.content?.trim();
  if (content && !isGenericNarration(content)) blocks.push(content);

  if (table) {
    // 포맷이 선언된 컬럼 또는 값이 전부 숫자인 컬럼은 우측 정렬(---:).
    const rightAlign = table.columns.map((col) => {
      if (table.formats[col]) return true;
      const vals = table.rows
        .map((r) => r[col])
        .filter((v) => v !== null && v !== undefined && v !== "");
      return vals.length > 0 && vals.every(isNumericLike);
    });
    const header = `| ${table.columns.map((c) => mdCell(table.labels[c] ?? c)).join(" | ")} |`;
    const sep = `| ${rightAlign.map((r) => (r ? "---:" : "---")).join(" | ")} |`;
    const body = table.rows
      .slice(0, COPY_MAX_ROWS)
      .map(
        (row) =>
          `| ${table.columns
            .map((c) => mdCell(formatCellValue(row[c], table.formats[c])))
            .join(" | ")} |`,
      );
    let tableBlock = [header, sep, ...body].join("\n");
    if (table.rows.length > COPY_MAX_ROWS) {
      tableBlock += `\n\n외 ${table.rows.length - COPY_MAX_ROWS}건`;
    }
    blocks.push(tableBlock);
  }

  // 모두 비면(표·의미요약 없음) generic이라도 본문을 fallback으로.
  if (blocks.length === 0) return content ?? "";
  return blocks.join("\n\n");
}

function csvEscape(value: unknown): string {
  const s = value == null ? "" : String(value);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function safeFilename(name: string): string {
  const cleaned = name.trim().replace(/[\\/:*?"<>|]+/g, "_").slice(0, 80);
  return cleaned || "분석결과";
}

export interface CsvExport {
  filename: string;
  content: string;
}

// 표 CSV(원자료). 값은 가공 없이 raw로 — 재import/스프레드시트 계산용.
// 표 데이터가 없으면 null. Excel 한글 깨짐 방지를 위해 UTF-8 BOM 선두 부착.
export function buildCsv(message: ChatMessage): CsvExport | null {
  const table = resultTable(message);
  if (!table) return null;
  const lines = [table.columns.map(csvEscape).join(",")];
  for (const row of table.rows) {
    lines.push(table.columns.map((c) => csvEscape(row[c])).join(","));
  }
  return {
    filename: `${safeFilename(table.title ?? "분석결과")}.csv`,
    content: "﻿" + lines.join("\r\n"),
  };
}

// 결과에 복사/다운로드 가능한 표 데이터가 있는지.
export function hasTableData(message: ChatMessage): boolean {
  return resultTable(message) !== null;
}

// Blob → 임시 <a>로 다운로드 트리거.
export function downloadCsv(csv: CsvExport): void {
  const blob = new Blob([csv.content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = csv.filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
