import type { ChatChart } from "../models";
import { scaleForChart } from "../models";
import DivergingBarView from "./DivergingBarView";
import LineTrendView from "./LineTrendView";
import RankingBarView from "./RankingBarView";

// 증감(delta) 차트 여부 — diverging_bar kind, 또는 (구버전 데이터 호환) point
// 포맷/음수 값. 증감 차트는 0 기준 다이버징 막대로 그린다.
function isDivergingChart(chart: ChatChart): boolean {
  if (chart.kind === "diverging_bar") return true;
  if (chart.kind !== "bar") return false;
  if (chart.yFormat === "point") return true;
  return chart.rows.some((row) => {
    const v = scaleForChart(row[chart.y], chart.yFormat);
    return v !== null && v < 0;
  });
}

// 차트 라우터 — kind별 전용 뷰로 위임 (silverone 2026-06-09 result view contract).
//   diverging_bar / 음수 delta → DivergingBarView (0 기준 다이버징)
//   line                       → LineTrendView (영역+점+기준일)
//   bar                        → RankingBarView (가로 랭킹)
export default function ChartView({ chart }: { chart: ChatChart }) {
  if (isDivergingChart(chart)) return <DivergingBarView chart={chart} />;
  if (chart.kind === "line") return <LineTrendView chart={chart} />;
  return <RankingBarView chart={chart} />;
}
