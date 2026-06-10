// silverone 2026-06-09 — result view 색/라벨 토큰 단일 source.
// 뷰마다 색이 흩어져 "증가" 의미가 뷰별로 달라지던 문제(예: diverging=초록 증가,
// metric=빨강 증가)를 막는다. 색 의미를 바꾸려면 이 파일만 고친다.

// ── 증감 방향 (가치판단 아님 — 단순 증가/감소 표현) ──────────────────────────
// 앱 전체 canonical: 증가=초록 / 감소=빨강 (diverging 차트 디자인 기준).
// 한국 증감관례(증가=빨강)로 뒤집으려면 아래 두 줄만 교체하면 전 뷰에 반영된다.
export const CHANGE_UP = "#3f9e6a"; // 증가 (green)
export const CHANGE_DOWN = "#d65a5a"; // 감소 (red)
// Tailwind text 클래스 버전(카드 텍스트용) — 위 hex와 같은 색 계열.
export const CHANGE_UP_TEXT = "text-emerald-600";
export const CHANGE_DOWN_TEXT = "text-rose-600";

// ── 시리즈/중립 색 ───────────────────────────────────────────────────────────
export const SERIES_BAR = "#8b7cf6"; // 랭킹/건수 막대 (purple)
export const SERIES_TRACK = "#f1f0fb"; // 막대 트랙 배경
export const SERIES_LINE = "#3b82f6"; // 추이 라인 (blue)
export const EVENT_LINE = "#d9a05b"; // 기준일(축제일) 기준선 (orange)

// ── 감성(긍정/중립/부정) ─────────────────────────────────────────────────────
// "증가/감소"와 의미가 다르다 — 감성은 범주, 증감은 방향.
export const SENTIMENT_LABEL: Record<string, string> = {
  positive: "긍정",
  neutral: "중립",
  negative: "부정",
};

// 감성 badge용 Tailwind 클래스 (EvidenceCardList).
export const SENTIMENT_BADGE_CLASS: Record<string, string> = {
  positive: "bg-emerald-50 text-emerald-700",
  neutral: "bg-zinc-100 text-zinc-600",
  negative: "bg-rose-50 text-rose-700",
};
export const SENTIMENT_BADGE_FALLBACK = "bg-zinc-100 text-zinc-600";
