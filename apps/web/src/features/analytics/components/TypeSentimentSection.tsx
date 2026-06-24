import { ThumbsDown, ThumbsUp } from "lucide-react";
import { BarTrack } from "@/components/common/charts";
import { fmt, pct, type ScopeData, type TypeDatum } from "../mock";
import { Block, BlockTitle, CardInsight } from "./primitives";

// 유형별 (감성비중 기준) 절 수 = n * 비중%. 긍/부정 각각 순위 정렬.
function rankBy(types: TypeDatum[], key: "pos" | "neg") {
  return types
    .map((t) => ({ name: t.name, v: Math.round((t.n * t[key]) / 100) }))
    .sort((a, b) => b.v - a.v);
}

function RankColumn({
  tone,
  rows,
}: {
  tone: "pos" | "neg";
  rows: { name: string; v: number }[];
}) {
  const isPos = tone === "pos";
  const max = Math.max(...rows.map((r) => r.v), 1);
  const total = rows.reduce((a, r) => a + r.v, 0);
  const Icon = isPos ? ThumbsUp : ThumbsDown;

  return (
    <div className="rounded-xl border border-zinc-100 bg-zinc-50/50 p-4">
      <div
        className={`mb-3.5 flex items-center gap-2 text-[13px] font-bold ${
          isPos ? "text-emerald-700" : "text-red-600"
        }`}
      >
        <span
          className={`grid h-5.5 w-5.5 place-items-center rounded-lg ${
            isPos ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-600"
          }`}
        >
          <Icon className="h-3.5 w-3.5" />
        </span>
        {isPos ? "긍정 많은 유형" : "부정 많은 유형"}
        <span className="ml-auto text-[11.5px] font-semibold text-zinc-400">
          총 {fmt(total)}
        </span>
      </div>

      <div className="flex flex-col gap-3">
        {rows.map((r, i) => (
          <div key={r.name} className="flex flex-col gap-1.5">
            <div className="flex items-center gap-2">
              <span
                className={`grid h-4.5 w-4.5 shrink-0 place-items-center rounded-md text-[11px] font-extrabold ${
                  i === 0
                    ? isPos
                      ? "bg-emerald-500 text-white"
                      : "bg-red-500 text-white"
                    : "border border-zinc-200 bg-white text-zinc-500"
                }`}
              >
                {i + 1}
              </span>
              <span className="truncate text-[13px] font-semibold text-zinc-800">
                {r.name}
              </span>
              <span className="ml-auto shrink-0 text-[13.5px] font-extrabold tabular-nums text-zinc-900">
                {fmt(r.v)}
                <span className="ml-1 text-[11px] font-bold text-zinc-400">
                  {pct(r.v, total)}%
                </span>
              </span>
            </div>
            <BarTrack
              className="h-3"
              percent={(r.v / max) * 100}
              fillClassName={`transition-all duration-500 ${
                isPos ? "bg-emerald-500" : "bg-red-500"
              }`}
            />
          </div>
        ))}
      </div>
    </div>
  );
}

export default function TypeSentimentSection({ data }: { data: ScopeData }) {
  const { types } = data;
  const posRows = rankBy(types, "pos");
  const negRows = rankBy(types, "neg");

  const topPos = [...types].sort((a, b) => b.pos - a.pos)[0];
  const negHeavy = types
    .filter((t) => t.neg >= 50)
    .sort((a, b) => b.neg - a.neg)
    .map((t) => t.name);

  return (
    <Block>
      <div className="mb-4.5">
        <BlockTitle title="유형별 감성 구성·대비" unit="clause" />
      </div>

      {/* 1) 100% 누적 (중립 제외) */}
      <div className="mb-1 flex items-center gap-1.5 text-[12.5px] font-bold text-zinc-600">
        <span className="font-extrabold text-zinc-400">1)</span>
        유형별 긍정 · 부정 구성비 (100% 기준)
      </div>
      <p className="mb-3.5 text-[12.5px] font-medium text-zinc-400">
        중립을 제외하고 각 유형의 긍정·부정 절만을 100%로 두어 비율을 비교합니다.
      </p>
      <div className="flex flex-col gap-3">
        {types.map((t) => {
          const base = t.pos + t.neg;
          const pp = base > 0 ? Math.round((t.pos / base) * 100) : 0;
          const np = 100 - pp;
          return (
            <div
              key={t.name}
              className="grid grid-cols-[138px_1fr] items-center gap-4"
            >
              <div className="truncate text-[13.5px] font-semibold text-zinc-800">
                {t.name}
              </div>
              <div className="flex h-6.5 overflow-hidden rounded-md text-[11px] font-extrabold text-white">
                <span
                  className="grid place-items-center bg-emerald-500 transition-all duration-500"
                  style={{ width: `${pp}%` }}
                >
                  {pp >= 8 ? `${pp}%` : ""}
                </span>
                <span
                  className="grid place-items-center bg-red-500 transition-all duration-500"
                  style={{ width: `${np}%` }}
                >
                  {np >= 8 ? `${np}%` : ""}
                </span>
              </div>
            </div>
          );
        })}
      </div>
      <div className="mt-3.5 flex gap-4.5 border-t border-zinc-100 pt-3.5 text-[12.5px] font-semibold text-zinc-500">
        <span className="inline-flex items-center gap-1.75">
          <i className="h-2.75 w-2.75 rounded-sm bg-emerald-500" />
          긍정
        </span>
        <span className="inline-flex items-center gap-1.75">
          <i className="h-2.75 w-2.75 rounded-sm bg-red-500" />
          부정
        </span>
        <span className="ml-auto text-zinc-400">※ 중립 절 제외 기준</span>
      </div>

      <div className="my-6 h-px bg-zinc-100" />

      {/* 2) 긍/부정 순위 대비 */}
      <div className="mb-1 flex items-center gap-1.5 text-[12.5px] font-bold text-zinc-600">
        <span className="font-extrabold text-zinc-400">2)</span>
        유형별 긍정 · 부정 대비 (절 수 순위)
      </div>
      <p className="mb-3.5 text-[12.5px] font-medium text-zinc-400">
        왼쪽은 긍정 절이 많은 유형, 오른쪽은 부정 절이 많은 유형 순으로
        정렬했습니다.
      </p>
      <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
        <RankColumn tone="pos" rows={posRows} />
        <RankColumn tone="neg" rows={negRows} />
      </div>

      <CardInsight>
        <b>{topPos.name}</b> 유형은 긍정 비율이 가장 높은 반면,{" "}
        {negHeavy.length > 0 ? (
          <>
            <b>{negHeavy.join(" · ")}</b> 유형은 부정 비율이 50%를 넘어 개선
            우선순위가 높습니다.
          </>
        ) : (
          <>부정 비율이 50%를 넘는 유형은 없습니다.</>
        )}
      </CardInsight>
    </Block>
  );
}
