// 보고서 PPTX 내보내기 — 이미지 박제가 아니라 **네이티브 PowerPoint 개체**로 생성한다.
// 리포트 블록이 구조화 데이터(ChatChart/ChatDisplay 표/metric/evidence/text)를 들고 있어
// PptxGenJS의 addChart/addTable/addText로 편집 가능한 개체를 만들 수 있다.
// 정책: 블록 1개 = 슬라이드 1장. 차트도 네이티브(bar/line/diverging_bar→col bar).
import type Pptx from "pptxgenjs";
import type { ReportBlock, LibraryItem } from "../models/editor";
import type { ReportResult } from "../models/result";
import type { ChatChart, ChatDisplay } from "@/features/chats/models";

// LAYOUT_WIDE = 13.33 x 7.5 inch.
const PAGE_W = 13.33;
const MARGIN = 0.5;
const CONTENT_W = PAGE_W - MARGIN * 2;
const BODY_BOTTOM = 7.1; // 본문이 차지할 수 있는 하단 한계.
const KR_FONT = "맑은 고딕"; // 한글 PPT 기본. 없으면 PowerPoint가 대체.
const MAX_TABLE_ROWS = 14; // 한 슬라이드에 표가 넘치지 않게 제한.

const toNum = (v: unknown): number => {
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : 0;
};
const cell = (v: unknown): string =>
  v === null || v === undefined ? "" : String(v);

const safeFilename = (title: string): string =>
  (title || "보고서").replace(/[\\/:*?"<>|]/g, "_");

/** 블록의 표시 제목 — 사용자 지정 > 라이브러리 원제목 > fallback. */
const blockTitle = (block: ReportBlock, lib?: LibraryItem): string =>
  (block.title && block.title.trim()) || lib?.title?.trim() || "분석 결과";

/** ChatChart → PptxGenJS 네이티브 차트 데이터. */
const chartData = (chart: ChatChart) => {
  const labels = chart.rows.map((r) => cell(r[chart.x]));
  const values = chart.rows.map((r) => toNum(r[chart.y]));
  return [{ name: chart.yLabel || chart.y || "값", labels, values }];
};

/** ChatDisplay(표) → 네이티브 표 rows([[cell]]). 헤더 1행 + 본문(상한 적용). */
const tableMatrix = (
  display: ChatDisplay,
): { rows: { text: string; options?: object }[][]; truncated: number } => {
  const cols = display.columns;
  const header = cols.map((c) => ({
    text: display.columnLabels?.[c] || c,
    options: { bold: true, fill: { color: "F1F5F9" }, color: "1E293B" },
  }));
  const bodySrc = display.rows.slice(0, MAX_TABLE_ROWS);
  const body = bodySrc.map((row) =>
    cols.map((c) => ({ text: cell(row[c]) })),
  );
  return {
    rows: [header, ...body],
    truncated: Math.max(0, display.rows.length - bodySrc.length),
  };
};

/** 한 블록을 한 슬라이드에 렌더. 우선순위: metric > evidence > chart > table > text. */
function renderBlock(
  pptx: Pptx,
  slide: ReturnType<Pptx["addSlide"]>,
  block: ReportBlock,
  lib: LibraryItem | undefined,
) {
  const result: ReportResult | undefined = lib?.result;

  // 제목.
  slide.addText(blockTitle(block, lib), {
    x: MARGIN,
    y: 0.3,
    w: CONTENT_W,
    h: 0.6,
    fontSize: 24,
    bold: true,
    color: "0F172A",
    fontFace: KR_FONT,
  });

  let cursorY = 1.05;
  // 원 질문 칩(opts.q).
  if (block.opts?.q && lib?.question?.trim()) {
    slide.addText(`Q. ${lib.question.trim()}`, {
      x: MARGIN,
      y: cursorY,
      w: CONTENT_W,
      h: 0.4,
      fontSize: 11,
      italic: true,
      color: "64748B",
      fontFace: KR_FONT,
    });
    cursorY += 0.45;
  }
  // 해석 문구.
  if (block.interp?.trim()) {
    slide.addText(block.interp.trim(), {
      x: MARGIN,
      y: cursorY,
      w: CONTENT_W,
      h: 0.8,
      fontSize: 13,
      color: "334155",
      fontFace: KR_FONT,
      valign: "top",
    });
    cursorY += 0.9;
  }

  const bodyY = cursorY + 0.1;
  const bodyH = Math.max(1.5, BODY_BOTTOM - bodyY);

  // 본문 개체.
  if (result?.metric) {
    const m = result.metric;
    const line = (label: string, v: number | null) =>
      v === null ? "" : `${label}: ${v.toLocaleString("ko-KR")}${m.unit ?? ""}`;
    const parts = [
      line("A", m.aValue),
      line("B", m.bValue),
      line("Δ", m.deltaValue),
      m.deltaRate === null ? "" : `증감률: ${m.deltaRate}%`,
    ].filter(Boolean);
    slide.addText(parts.join("\n") || "지표 없음", {
      x: MARGIN,
      y: bodyY,
      w: CONTENT_W,
      h: bodyH,
      fontSize: 28,
      bold: true,
      color: "0F172A",
      fontFace: KR_FONT,
      valign: "top",
      lineSpacingMultiple: 1.3,
    });
    return;
  }

  if (result?.evidence) {
    const items = result.evidence.items.slice(0, 10);
    slide.addText(
      items.map((it) => ({
        text: it.text + (it.sentiment ? `  (${it.sentiment})` : ""),
        options: { bullet: true, fontSize: 13, color: "334155", fontFace: KR_FONT },
      })),
      { x: MARGIN, y: bodyY, w: CONTENT_W, h: bodyH, valign: "top" },
    );
    return;
  }

  if (result?.chart) {
    const chart = result.chart;
    const type =
      chart.kind === "line" ? pptx.ChartType.line : pptx.ChartType.bar;
    slide.addChart(type, chartData(chart), {
      x: MARGIN,
      y: bodyY,
      w: CONTENT_W,
      h: bodyH,
      barDir: "col",
      showLegend: false,
      showTitle: false,
      showValue: false,
      catAxisLabelFontSize: 9,
      valAxisLabelFontSize: 9,
      chartColors: ["2563EB"],
      catAxisLabelFontFace: KR_FONT,
    });
    return;
  }

  if (result?.display) {
    const { rows, truncated } = tableMatrix(result.display);
    slide.addTable(rows, {
      x: MARGIN,
      y: bodyY,
      w: CONTENT_W,
      fontSize: 10,
      fontFace: KR_FONT,
      color: "334155",
      border: { type: "solid", pt: 0.5, color: "E2E8F0" },
      valign: "middle",
      autoPage: false,
    });
    if (truncated > 0) {
      slide.addText(`…외 ${truncated.toLocaleString("ko-KR")}행 (전체는 표/CSV에서 확인)`, {
        x: MARGIN,
        y: BODY_BOTTOM,
        w: CONTENT_W,
        h: 0.3,
        fontSize: 10,
        italic: true,
        color: "94A3B8",
        fontFace: KR_FONT,
      });
    }
    return;
  }

  // text 블록 등 시각 개체가 없으면 제목/해석만으로 충분(이미 위에 렌더됨).
}

/**
 * 보고서를 PPTX로 내보낸다. 블록 1개 = 슬라이드 1장, 네이티브 개체.
 * @returns 내보낸 슬라이드가 있으면 true.
 */
export async function exportReportPPTX(
  title: string,
  blocks: ReportBlock[],
  libById: (id: string) => LibraryItem | undefined,
): Promise<boolean> {
  if (!blocks.length) return false;
  const PptxGenJS = (await import("pptxgenjs")).default;
  const pptx = new PptxGenJS();
  pptx.layout = "LAYOUT_WIDE";
  pptx.author = "분석지원시스템";
  pptx.title = title || "분석 보고서";

  for (const block of blocks) {
    const slide = pptx.addSlide();
    try {
      renderBlock(pptx, slide, block, libById(block.libId));
    } catch (err) {
      // 한 블록 실패가 전체 내보내기를 막지 않도록 격리.
      slide.addText(
        `이 블록은 PPTX로 변환하지 못했어요.\n${(err as Error)?.message ?? ""}`,
        { x: MARGIN, y: 1, w: CONTENT_W, h: 1, fontSize: 12, color: "DC2626", fontFace: KR_FONT },
      );
    }
  }

  await pptx.writeFile({ fileName: `${safeFilename(title)}.pptx` });
  return true;
}
