import {
  DistributionLegend,
  DonutChart,
  type DonutDatum,
  type LegendDatum,
} from "@/components/common/charts";
import { fmt, pct, type ScopeData } from "../mock";
import {
  Block,
  BlockTitle,
  CardInsight,
  DataTableMini,
} from "./primitives";

export default function SentimentSection({ data }: { data: ScopeData }) {
  const { sentiment, clauseTotal: total } = data;
  const posPct = pct(sentiment[0].n, total);
  const negPct = pct(sentiment[2].n, total);
  const neuPct = pct(sentiment[1].n, total);

  const donut: DonutDatum[] = sentiment.map((s) => ({
    key: s.key,
    value: s.n,
    color: s.color,
  }));
  const legend: LegendDatum[] = sentiment.map((s) => ({
    key: s.key,
    label: s.name,
    value: s.n,
    percent: pct(s.n, total),
    color: s.color,
  }));

  return (
    <Block>
      <BlockTitle
        title="절 단위 감성 분포"
        sub={`총 ${fmt(total)}개 절 · 긍정 / 중립 / 부정`}
        unit="clause"
      />

      <div className="mt-4 grid grid-cols-1 gap-7 lg:grid-cols-[1fr_284px]">
        {/* 도넛 + 범례 */}
        <div className="flex flex-wrap items-center gap-7">
          <DonutChart
            data={donut}
            size={168}
            innerRadius={52}
            outerRadius={74}
            paddingAngle={3}
            center={
              <div className="text-center">
                <div className="text-2xl font-extrabold leading-none tabular-nums text-zinc-900">
                  {posPct}%
                </div>
                <div className="mt-1 text-[11px] font-semibold text-zinc-400">
                  긍정 비율
                </div>
              </div>
            }
          />
          <DistributionLegend items={legend} className="min-w-55 flex-1" />
        </div>

        {/* 표 */}
        <DataTableMini
          head={
            <thead>
              <tr className="bg-zinc-50/70 text-[11px] font-bold text-zinc-400">
                <th className="px-3 py-2.5 text-left">감성</th>
                <th className="px-3 py-2.5 text-right">절수</th>
                <th className="px-3 py-2.5 text-right">비율</th>
              </tr>
            </thead>
          }
        >
          <tbody>
            {sentiment.map((s) => (
              <tr key={s.key} className="border-t border-zinc-100">
                <td className="px-3 py-2.25 font-semibold text-zinc-800">
                  <span
                    className="mr-2 inline-block h-2.25 w-2.25 rounded-sm align-middle"
                    style={{ background: s.color }}
                  />
                  {s.name}
                </td>
                <td className="px-3 py-2.25 text-right font-bold tabular-nums text-zinc-900">
                  {fmt(s.n)}
                </td>
                <td className="px-3 py-2.25 text-right tabular-nums text-zinc-500">
                  {pct(s.n, total)}%
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t border-zinc-200 bg-zinc-50/70 font-extrabold text-zinc-900">
              <td className="px-3 py-2.5">합계</td>
              <td className="px-3 py-2.5 text-right tabular-nums">
                {fmt(total)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums">100%</td>
            </tr>
          </tfoot>
        </DataTableMini>
      </div>

      <CardInsight>
        절 단위 감성은 <b>긍정 {posPct}%</b>로 과반이며, 부정 {negPct}%가 중립{" "}
        {neuPct}%보다 높게 나타납니다.
      </CardInsight>
    </Block>
  );
}
