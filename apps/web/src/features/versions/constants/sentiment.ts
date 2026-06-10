// 감성 3분류(긍정·중립·부정) 공용 도메인 상수.
// 탭마다 중복 정의하던 색·라벨을 한 곳으로 모은다.
// (차트 컴포넌트는 색을 prop으로만 받으므로 이 상수에 의존하지 않는다.)

export type Sentiment = "positive" | "neutral" | "negative";

// 한글 라벨.
export const SENTIMENT_LABELS: Record<Sentiment, string> = {
  positive: "긍정",
  neutral: "중립",
  negative: "부정",
};

// 차트(도넛/막대) 색 — hex.
export const SENTIMENT_COLORS: Record<Sentiment, string> = {
  positive: "#10b981", // emerald-500
  neutral: "#a1a1aa", // zinc-400
  negative: "#ef4444", // red-500
};

// 도넛/범례 표시 순서 고정 (긍정 → 중립 → 부정).
export const SENTIMENT_ORDER: Sentiment[] = ["positive", "neutral", "negative"];

// 배지 배경·글자색 (Tailwind).
export const SENTIMENT_BADGE: Record<Sentiment, string> = {
  positive: "bg-emerald-50 text-emerald-600",
  neutral: "bg-zinc-100 text-zinc-500",
  negative: "bg-red-50 text-red-600",
};

export const SENTIMENT_FILTER_OPTIONS: { label: string; value: string }[] = [
  { label: "전체", value: "" },
  { label: "긍정", value: "positive" },
  { label: "중립", value: "neutral" },
  { label: "부정", value: "negative" },
];