import { BarTrack } from "@/components/common/charts";
import { fmt, pct, type ScopeData } from "../mock";
import {
  Block,
  BlockTitle,
  CardInsight,
  DataTableMini,
} from "./primitives";

export default function TypeSection({ data }: { data: ScopeData }) {
  const { types, clauseTotal: total } = data;
  const max = Math.max(...types.map((t) => t.n), 1);
  const top2Share = pct(types[0].n + (types[1]?.n ?? 0), total);

  return (
    <Block>
      <BlockTitle
        title="유형별 절 분포"
        sub={`총 ${fmt(total)}개 절 · 유형별 출현 빈도`}
        unit="clause"
      />

      <div className="mt-4 grid grid-cols-1 gap-7 lg:grid-cols-[1fr_284px]">
        {/* 막대 */}
        <div className="flex flex-col gap-3.25">
          {types.map((t) => (
            <div
              key={t.name}
              className="grid grid-cols-[138px_1fr_112px] items-center gap-4"
            >
              <div className="truncate text-[13.5px] font-semibold text-zinc-800">
                {t.name}
              </div>
              <BarTrack
                className="h-5 !rounded-md"
                percent={(t.n / max) * 100}
                fillClassName="!rounded-md bg-linear-to-r from-blue-500 to-blue-600 transition-all duration-500"
              />
              <div className="flex items-baseline justify-end gap-2 tabular-nums">
                <span className="text-[14.5px] font-extrabold text-zinc-900">
                  {fmt(t.n)}
                </span>
                <span className="text-[12px] font-bold text-zinc-400">
                  {pct(t.n, total)}%
                </span>
              </div>
            </div>
          ))}
        </div>

        {/* 표 */}
        <DataTableMini
          head={
            <thead>
              <tr className="bg-zinc-50/70 text-[11px] font-bold text-zinc-400">
                <th className="px-3 py-2.5 text-left">유형</th>
                <th className="px-3 py-2.5 text-right">절수</th>
                <th className="px-3 py-2.5 text-right">비율</th>
              </tr>
            </thead>
          }
        >
          <tbody>
            {types.map((t) => (
              <tr key={t.name} className="border-t border-zinc-100">
                <td className="px-3 py-2.25 font-semibold text-zinc-800">
                  {t.name}
                </td>
                <td className="px-3 py-2.25 text-right font-bold tabular-nums text-zinc-900">
                  {fmt(t.n)}
                </td>
                <td className="px-3 py-2.25 text-right tabular-nums text-zinc-500">
                  {pct(t.n, total)}%
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
        출현 빈도는 <b>{types[0].name}</b>·<b>{types[1]?.name}</b> 순으로 높아,
        상위 2개 유형이 전체 절의 <b>{top2Share}%</b>를 차지합니다.
      </CardInsight>
    </Block>
  );
}
