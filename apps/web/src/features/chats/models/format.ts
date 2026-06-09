// silverone 2026-06-09 — 기간/그룹 비교 결과 컬럼 표시 포맷. 백엔드 column_formats
// contract(percent/point/int/number)를 받아 %·%p·정수로 렌더한다. 백엔드가 의미를
// 선언하므로 프론트는 컬럼명을 추측하지 않는다.

export type ColumnFormat = "percent" | "point" | "int" | "number";

export function toColumnFormat(value: string | undefined): ColumnFormat | undefined {
  if (value === "percent" || value === "point" || value === "int" || value === "number") {
    return value;
  }
  return undefined;
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

// 차트 축/막대용 — 포맷이 percent/point면 0~1 비율을 0~100 스케일로 올린다.
// (그 외는 원값). null이면 null.
export function scaleForChart(value: unknown, format: ColumnFormat | undefined): number | null {
  const n = toNumber(value);
  if (n === null) return null;
  return format === "percent" || format === "point" ? n * 100 : n;
}

// percent/point의 표시 단위 ("%" / "%p"). 그 외 없음.
export function unitOf(format: ColumnFormat | undefined): string {
  if (format === "percent") return "%";
  if (format === "point") return "%p";
  return "";
}

// 표 셀/툴팁용 문자열. percent→56.6%, point→+28.7%p / -30.2%p, int→정수.
export function formatCellValue(value: unknown, format: ColumnFormat | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = toNumber(value);
  if (format && n !== null) {
    switch (format) {
      case "percent":
        return `${(n * 100).toFixed(1)}%`;
      case "point": {
        const sign = n > 0 ? "+" : "";
        return `${sign}${(n * 100).toFixed(1)}%p`;
      }
      case "int":
        return String(Math.round(n));
      case "number":
        return String(n);
    }
  }
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}
