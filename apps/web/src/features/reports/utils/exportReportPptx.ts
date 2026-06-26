// 보고서 PPTX 내보내기 — 이미지 박제가 아니라 **네이티브 PowerPoint 개체**로 생성한다.
// 리포트 블록이 구조화 데이터(ChatChart/ChatDisplay 표/metric/evidence/text)를 들고 있어
// PptxGenJS의 addChart/addTable/addText로 편집 가능한 개체를 만들 수 있다.
// 정책: 블록 1개 = 슬라이드 1장. 차트도 네이티브(bar/line/diverging_bar→col bar).
import type Pptx from "pptxgenjs";
import type { ReportBlock, TemplatePanel } from "../models/editor";
import { projectResult, type ReportResult } from "../models/result";
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

/** 블록의 표시 제목 — 사용자 지정 > 스냅샷 기본 제목 > fallback. */
const blockTitle = (block: ReportBlock): string => {
  if (block.title && block.title.trim()) return block.title.trim();
  if (block.kind === "section")
    return block.section?.defaultTitle?.trim() || "섹션";
  return block.result?.defaultTitle?.trim() || "분석 결과";
};

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

// ── 기본 템플릿 섹션 패널 → PPTX 네이티브 개체 ─────────────────────────
// 섹션 블록(kind==="section")은 여러 패널(rows×panels)을 들고 있다. 에디터 카드처럼
// 한 슬라이드에 width 기반 그리드로 배치하고, 각 패널을 view에 맞는 네이티브
// 개체(막대/도넛/스택 차트·표)로 렌더한다. 계약: docs/api/report_basic_template.sample.md.

const COLS = 12;
const WIDTH_SPAN: Record<string, number> = {
  full: 12,
  "3/4": 9,
  "2/3": 8,
  "1/2": 6,
  "1/3": 4,
  "1/4": 3,
};
const PANEL_GAP = 0.18; // 같은 행 패널 사이 가로 간격(inch).
const PANEL_TITLE_H = 0.34;

interface DistItem {
  key: string;
  label: string;
  count: number;
  percent: number;
}
interface DistData {
  total: number;
  items: DistItem[];
}
interface StatItemData {
  key: string;
  label: string;
  value: unknown;
  format?: string;
  unit?: string;
  sub?: unknown;
}
interface StackedCat {
  key: string;
  label: string;
  total: number;
}
interface StackedSeries {
  key: string;
  label: string;
  counts: number[];
  percents: number[];
}
interface StackedData {
  categories: StackedCat[];
  series: StackedSeries[];
}
interface RankItem {
  rank: number;
  label: string;
  value: number;
}

type Rect = { x: number; y: number; w: number; h: number };
type Slide = ReturnType<Pptx["addSlide"]>;

// key별 고정 색(감성) + 팔레트 순환. BasicReportLayout과 동일 매핑(HEX, '#' 없이).
const KEY_COLOR: Record<string, string> = {
  positive: "10B981",
  neutral: "A1A1AA",
  negative: "EF4444",
};
const PALETTE = ["7C3AED", "2563EB", "10B981", "F59E0B", "EF4444", "06B6D4", "EC4899", "A1A1AA"];
const hexFor = (key: string, i: number): string => KEY_COLOR[key] ?? PALETTE[i % PALETTE.length];

// stat_grid 값 표현 — BasicReportLayout.formatValue + unit 결합.
const fmtStat = (v: unknown, format?: string, unit?: string): string => {
  if (v === null || v === undefined || v === "") return "—";
  let s: string;
  if (format === "count") s = typeof v === "number" ? v.toLocaleString("ko-KR") : String(v);
  else if (format === "percent") s = `${v}%`;
  else if (format === "ratio") s = typeof v === "number" ? v.toFixed(2) : String(v);
  else s = String(v);
  return unit ? `${s}${unit}` : s;
};

const thCell = (text: string, align?: "right") => ({
  text,
  options: { bold: true, fill: { color: "F1F5F9" }, color: "1E293B", ...(align ? { align } : {}) },
});

/** 패널 제목 줄을 그리고 본문 시작 y를 반환. */
function drawPanelTitle(slide: Slide, panel: TemplatePanel, rect: Rect): number {
  if (!panel.title?.trim()) return rect.y;
  slide.addText(panel.title.trim(), {
    x: rect.x,
    y: rect.y,
    w: rect.w,
    h: PANEL_TITLE_H,
    fontSize: 12,
    bold: true,
    color: "475569",
    fontFace: KR_FONT,
    valign: "middle",
  });
  return rect.y + PANEL_TITLE_H + 0.05;
}

/** 패널 1개를 rect 안에 view별 네이티브 개체로 렌더. */
function renderPanel(pptx: Pptx, slide: Slide, panel: TemplatePanel, rect: Rect) {
  const top = drawPanelTitle(slide, panel, rect);
  const bodyH = Math.max(0.6, rect.y + rect.h - top);
  const data = (panel.data ?? {}) as Record<string, unknown>;

  if (panel.view === "doughnut") {
    const items = ((data as unknown as DistData).items ?? []) as DistItem[];
    if (!items.length) return;
    slide.addChart(
      pptx.ChartType.doughnut,
      [{ name: "분포", labels: items.map((it) => it.label), values: items.map((it) => it.count) }],
      {
        x: rect.x, y: top, w: rect.w, h: bodyH,
        showLegend: true, legendPos: "r", legendFontFace: KR_FONT,
        showValue: false, showPercent: true, dataLabelFontFace: KR_FONT, dataLabelFontSize: 9,
        chartColors: items.map((it, i) => hexFor(it.key, i)),
        holeSize: 55,
      },
    );
    return;
  }

  if (panel.view === "bar") {
    const items = ((data as unknown as DistData).items ?? []) as DistItem[];
    if (!items.length) return;
    slide.addChart(
      pptx.ChartType.bar,
      [{ name: "수", labels: items.map((it) => it.label), values: items.map((it) => it.count) }],
      {
        x: rect.x, y: top, w: rect.w, h: bodyH,
        barDir: "bar", showLegend: false, showValue: true,
        dataLabelFontSize: 9, dataLabelFontFace: KR_FONT,
        catAxisLabelFontSize: 9, catAxisLabelFontFace: KR_FONT, valAxisHidden: true,
        chartColors: ["7C3AED"],
      },
    );
    return;
  }

  if (panel.view === "stacked_bar") {
    const d = data as unknown as StackedData;
    const cats = d.categories ?? [];
    const series = d.series ?? [];
    if (!cats.length || !series.length) return;
    slide.addChart(
      pptx.ChartType.bar,
      series.map((s) => ({ name: s.label, labels: cats.map((c) => c.label), values: s.counts })),
      {
        x: rect.x, y: top, w: rect.w, h: bodyH,
        barDir: "bar", barGrouping: "percentStacked",
        showLegend: true, legendPos: "b", legendFontFace: KR_FONT,
        catAxisLabelFontSize: 9, catAxisLabelFontFace: KR_FONT, valAxisHidden: true,
        chartColors: series.map((s, i) => hexFor(s.key, i)),
      },
    );
    return;
  }

  if (panel.view === "table") {
    const d = data as unknown as DistData;
    const items = d.items ?? [];
    const body = items.map((it) => [
      { text: it.label },
      { text: it.count.toLocaleString("ko-KR"), options: { align: "right" as const } },
      { text: `${it.percent}%`, options: { align: "right" as const } },
    ]);
    const foot = [
      { text: "합계", options: { bold: true, fill: { color: "F8FAFC" } } },
      { text: (d.total ?? 0).toLocaleString("ko-KR"), options: { bold: true, align: "right" as const, fill: { color: "F8FAFC" } } },
      { text: "100%", options: { bold: true, align: "right" as const, fill: { color: "F8FAFC" } } },
    ];
    slide.addTable([[thCell("항목"), thCell("수", "right"), thCell("비율", "right")], ...body, foot], {
      x: rect.x, y: top, w: rect.w, fontSize: 9, fontFace: KR_FONT, color: "334155",
      border: { type: "solid", pt: 0.5, color: "E2E8F0" }, valign: "middle", autoPage: false,
    });
    return;
  }

  if (panel.view === "rank") {
    const items = [...(((data as unknown as { items?: RankItem[] }).items) ?? [])].sort(
      (a, b) => a.rank - b.rank,
    );
    const body = items.map((it) => [
      { text: String(it.rank) },
      { text: it.label },
      { text: it.value.toLocaleString("ko-KR"), options: { align: "right" as const } },
    ]);
    slide.addTable([[thCell("순위"), thCell("항목"), thCell("값", "right")], ...body], {
      x: rect.x, y: top, w: rect.w, fontSize: 9, fontFace: KR_FONT, color: "334155",
      border: { type: "solid", pt: 0.5, color: "E2E8F0" }, valign: "middle", autoPage: false,
    });
    return;
  }

  if (panel.view === "stat_grid") {
    const items = (((data as unknown as { items?: StatItemData[] }).items) ?? []) as StatItemData[];
    if (!items.length) return;
    const rows = items.map((it) => [
      { text: it.label, options: { color: "64748B" } },
      {
        text:
          fmtStat(it.value, it.format, it.unit) +
          (it.sub !== null && it.sub !== undefined && it.sub !== "" ? `  (${String(it.sub)})` : ""),
        options: { bold: true, color: "0F172A", align: "right" as const },
      },
    ]);
    slide.addTable(rows, {
      x: rect.x, y: top, w: rect.w, fontSize: 11, fontFace: KR_FONT,
      border: { type: "solid", pt: 0.5, color: "EEF2F7" }, valign: "middle", autoPage: false,
      colW: [rect.w * 0.5, rect.w * 0.5],
    });
    return;
  }

  if (panel.view === "text") {
    const md = (data as unknown as { markdown?: string }).markdown;
    if (md?.trim()) {
      slide.addText(md.trim(), {
        x: rect.x, y: top, w: rect.w, h: bodyH, fontSize: 12, color: "334155",
        fontFace: KR_FONT, valign: "top",
      });
    }
    return;
  }
}

/** 기본 템플릿 섹션 블록 → 슬라이드 1장(패널을 width 그리드로 배치). */
function renderSectionSlide(pptx: Pptx, block: ReportBlock) {
  const slide = pptx.addSlide();
  const section = block.section;
  const scope = section?.scopeLabel?.trim();
  slide.addText(blockTitle(block) + (scope ? `   ${scope}` : ""), {
    x: MARGIN, y: 0.3, w: CONTENT_W, h: 0.6, fontSize: 24, bold: true,
    color: "0F172A", fontFace: KR_FONT,
  });

  let top = 1.05;
  if (block.interp?.trim()) {
    slide.addText(block.interp.trim(), {
      x: MARGIN, y: top, w: CONTENT_W, h: 0.6, fontSize: 13, color: "334155",
      fontFace: KR_FONT, valign: "top",
    });
    top += 0.7;
  }

  const rows = section?.rows ?? [];
  if (!rows.length) return;
  const bandH = Math.max(1.2, (BODY_BOTTOM - top) / rows.length);
  rows.forEach((row, ri) => {
    const y = top + ri * bandH;
    const spans = row.panels.map((p) => WIDTH_SPAN[p.width] ?? COLS);
    // 합이 12 미만이어도 12 기준 좌측 정렬(에디터 그리드와 동일 폭).
    const totalSpan = Math.max(spans.reduce((a, b) => a + b, 0), COLS);
    let x = MARGIN;
    row.panels.forEach((panel, pi) => {
      const slot = CONTENT_W * (spans[pi] / totalSpan);
      const w = slot - (pi < row.panels.length - 1 ? PANEL_GAP : 0);
      renderPanel(pptx, slide, panel, { x, y, w, h: bandH - 0.2 });
      x += slot;
    });
  });
}

/** 한 블록을 한 슬라이드에 렌더. 우선순위: metric > evidence > chart > table > text. */
function renderBlock(
  pptx: Pptx,
  slide: ReturnType<Pptx["addSlide"]>,
  block: ReportBlock,
) {
  const result: ReportResult | undefined = block.result
    ? projectResult(block.result)
    : undefined;
  const question = block.result?.question;

  // 제목.
  slide.addText(blockTitle(block), {
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
  if (block.opts?.q && question?.trim()) {
    slide.addText(`Q. ${question.trim()}`, {
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

  // section 블록은 renderSectionSlide가 처리하므로 여기 도달하지 않는다. result/text
  // 블록은 시각 개체가 없으면 제목/해석만으로 충분(이미 위에 렌더됨).
}

/**
 * 보고서를 PPTX로 내보낸다. 블록 1개 = 슬라이드 1장, 네이티브 개체.
 * @returns 내보낸 슬라이드가 있으면 true.
 */
export async function exportReportPPTX(
  title: string,
  blocks: ReportBlock[],
): Promise<boolean> {
  if (!blocks.length) return false;
  const PptxGenJS = (await import("pptxgenjs")).default;
  const pptx = new PptxGenJS();
  pptx.layout = "LAYOUT_WIDE";
  pptx.author = "분석지원시스템";
  pptx.title = title || "분석 보고서";

  for (const block of blocks) {
    try {
      if (block.kind === "section" && block.section) {
        // 기본 템플릿 섹션 — 패널을 네이티브 차트/표 그리드로 한 슬라이드에 렌더.
        renderSectionSlide(pptx, block);
      } else {
        const slide = pptx.addSlide();
        renderBlock(pptx, slide, block);
      }
    } catch (err) {
      // 한 블록 실패가 전체 내보내기를 막지 않도록 격리.
      const slide = pptx.addSlide();
      slide.addText(
        `이 블록은 PPTX로 변환하지 못했어요.\n${(err as Error)?.message ?? ""}`,
        { x: MARGIN, y: 1, w: CONTENT_W, h: 1, fontSize: 12, color: "DC2626", fontFace: KR_FONT },
      );
    }
  }

  await pptx.writeFile({ fileName: `${safeFilename(title)}.pptx` });
  return true;
}
