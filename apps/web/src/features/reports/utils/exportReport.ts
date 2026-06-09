// 보고서 내보내기 유틸. 캔버스(미리보기) DOM 스냅샷을 기반으로 한다.
// HTML: 현재 페이지의 모든 CSS 규칙을 인라인해 단일 파일로 저장.
// PDF: 보고서 영역만 보이도록 print 스타일을 주입한 뒤 브라우저 인쇄(파일로 저장).
// NOTE: 디자인 샘플 단계의 클라이언트 내보내기. 실제 서버 렌더링/포맷 변환은 추후 연동.

const EXPORT_ROOT_ID = "report-export-root";

function esc(s: string): string {
  return s.replace(
    /[&<>"]/g,
    (c) =>
      (({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }) as Record<
        string,
        string
      >)[c],
  );
}

// 현재 문서의 CSS 규칙 전체를 텍스트로 수집(Tailwind 컴파일 결과 포함).
function collectCss(): string {
  let css = "";
  for (const sheet of Array.from(document.styleSheets)) {
    try {
      for (const rule of Array.from(sheet.cssRules)) css += rule.cssText + "\n";
    } catch {
      // cross-origin 스타일시트는 접근 불가 → 건너뜀.
    }
  }
  return css;
}

function triggerDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** 캔버스 보고서 영역을 단일 HTML 파일로 저장 */
export function exportReportHTML(title: string): boolean {
  const root = document.getElementById(EXPORT_ROOT_ID);
  if (!root) return false;
  const css = collectCss();
  const doc = `<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${esc(
    title,
  )}</title><style>${css}
  body{background:#fff;}
  .report-export-wrap{max-width:820px;margin:0 auto;padding:48px 24px 80px;}
  </style></head><body><div class="report-export-wrap">${root.innerHTML}</div></body></html>`;
  const safe = (title || "보고서").replace(/[\\/:*?"<>|]/g, "_");
  triggerDownload(
    new Blob([doc], { type: "text/html;charset=utf-8" }),
    `${safe}.html`,
  );
  return true;
}

/** 보고서 영역만 인쇄(브라우저 인쇄 → PDF로 저장) */
export function exportReportPDF() {
  const style = document.createElement("style");
  style.id = "report-print-style";
  style.textContent = `@media print {
    body * { visibility: hidden !important; }
    #${EXPORT_ROOT_ID}, #${EXPORT_ROOT_ID} * { visibility: visible !important; }
    #${EXPORT_ROOT_ID} { position: absolute !important; left: 0; top: 0; width: 100%; }
    @page { margin: 16mm; }
  }`;
  document.head.appendChild(style);
  const cleanup = () => {
    style.remove();
    window.removeEventListener("afterprint", cleanup);
  };
  window.addEventListener("afterprint", cleanup);
  window.print();
  // afterprint 미발생 환경 대비 안전망.
  setTimeout(cleanup, 1000);
}

export const REPORT_EXPORT_ROOT_ID = EXPORT_ROOT_ID;
