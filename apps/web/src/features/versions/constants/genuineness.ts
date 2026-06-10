// 문서 진성 3-tier 분류 도메인 상수.
// key는 백엔드 genuineness 컬럼 원본(snake_case)과 일치해야 서버 필터가 동작한다.
// mixed는 planner가 더 이상 생성하지 않아(backward-compat enum만 존재) UI에서 제거.

export type Genuineness = "genuine_review" | "non_review" | "uncertain";

// 한글 라벨.
export const GENUINENESS_LABELS: Record<Genuineness, string> = {
  genuine_review: "진성",
  non_review: "비진성",
  uncertain: "불확실",
};

// 차트(도넛/범례) 색 — hex.
export const GENUINENESS_COLORS: Record<Genuineness, string> = {
  genuine_review: "#10b981", // emerald-500
  non_review: "#f87171", // red-400
  uncertain: "#a1a1aa", // zinc-400
};

// 배지 배경·글자색 (Tailwind).
export const GENUINENESS_BADGE: Record<Genuineness, string> = {
  genuine_review: "bg-emerald-50 text-emerald-600",
  non_review: "bg-red-50 text-red-600",
  uncertain: "bg-zinc-100 text-zinc-500",
};

// 요약/범례 표시 순서 고정 (진성 → 비진성 → 불확실).
export const GENUINENESS_ORDER: Genuineness[] = [
  "genuine_review",
  "non_review",
  "uncertain",
];
