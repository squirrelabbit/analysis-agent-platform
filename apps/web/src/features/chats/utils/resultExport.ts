// 결과 카드 액션(복사·다운로드)용 직렬화 유틸. (silverone 2026-06-11)
// display(표)가 있으면 그 컬럼/행을, 없고 chart만 있으면 chart의 x/y를 데이터로
// 사용한다. metric/evidence-only 결과는 표 데이터가 없어 텍스트(content)만 복사.
import type { ChatMessage } from "../models";

interface ResultTable {
  columns: string[];
  rows: Record<string, unknown>[];
  title?: string;
}

// 결과에서 표 형태 데이터를 추출. 없으면 null.
function resultTable(message: ChatMessage): ResultTable | null {
  const d = message.display;
  if (d && d.columns.length > 0) {
    return { columns: d.columns, rows: d.rows, title: d.title };
  }
  const c = message.chart;
  if (c && c.rows.length > 0) {
    return { columns: [c.x, c.y], rows: c.rows, title: c.title };
  }
  return null;
}

function cellText(value: unknown): string {
  if (value == null) return "";
  return String(value);
}

// 클립보드 복사용 텍스트. 답변 본문 + (있으면) 탭 구분 표.
export function buildCopyText(message: ChatMessage): string {
  const parts: string[] = [];
  const content = message.content?.trim();
  if (content) parts.push(content);
  const table = resultTable(message);
  if (table) {
    if (table.title) parts.push(table.title);
    parts.push(table.columns.join("\t"));
    for (const row of table.rows) {
      parts.push(table.columns.map((c) => cellText(row[c])).join("\t"));
    }
  }
  return parts.join("\n");
}

function csvEscape(value: unknown): string {
  const s = cellText(value);
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

// 표 CSV. 표 데이터가 없으면 null(다운로드 버튼은 그 경우 토스트로 안내).
// Excel 한글 깨짐 방지를 위해 UTF-8 BOM 선두 부착.
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
