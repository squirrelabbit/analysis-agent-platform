import { ThumbsDown, ThumbsUp } from "lucide-react";
import { BarTrack } from "@/components/common/charts";
import { fmt, type KeywordRow } from "../mock";
import { Block, BlockTitle } from "./primitives";

function KeywordColumn({
  tone,
  rows,
}: {
  tone: "pos" | "neg";
  rows: KeywordRow[];
}) {
  const isPos = tone === "pos";
  const top10 = rows.slice(0, 10);
  const max = top10[0]?.[1] ?? 1;
  const Icon = isPos ? ThumbsUp : ThumbsDown;

  return (
    <div className="overflow-hidden rounded-xl border border-zinc-100">
      <div
        className={`flex items-center gap-2 px-4 py-3 text-[13px] font-bold ${
          isPos
            ? "bg-emerald-50 text-emerald-700"
            : "bg-red-50 text-red-600"
        }`}
      >
        <span className="grid h-5.5 w-5.5 place-items-center rounded-lg bg-white">
          <Icon className="h-3.5 w-3.5" />
        </span>
        {isPos ? "긍정 키워드" : "부정 키워드"}
        <span className="ml-auto text-[11px] font-bold text-zinc-400">
          Top 10
        </span>
      </div>

      <table className="w-full border-collapse text-[12.5px]">
        <tbody>
          {top10.map(([word, n], i) => (
            <tr key={word} className="border-t border-zinc-100">
              <td
                className={`w-9 px-2 py-2.25 text-center font-extrabold tabular-nums ${
                  i < 3 ? "text-zinc-900" : "text-zinc-400"
                }`}
              >
                {i + 1}
              </td>
              <td className="px-1 py-2.25 font-semibold text-zinc-900">
                {word}
              </td>
              <td className="px-2 py-2.25 text-right font-bold tabular-nums text-zinc-900">
                {fmt(n)}
              </td>
              {/* <td className="w-24 px-3 py-2.25">
                <BarTrack
                  className="h-2"
                  percent={(n / max) * 100}
                  fillClassName={isPos ? "bg-emerald-500" : "bg-red-500"}
                />
              </td> */}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function KeywordSection({
  kwPos,
  kwNeg,
}: {
  kwPos: KeywordRow[];
  kwNeg: KeywordRow[];
}) {
  return (
    <Block>
      <div className="mb-4.5">
        <BlockTitle
          title="감성별 상위 키워드"
          sub="긍정 · 부정 절에서 추출한 키워드 빈도 상위 10개"
          unit="clause"
        />
      </div>
      <div className="grid grid-cols-1 gap-7 sm:grid-cols-2">
        <KeywordColumn tone="pos" rows={kwPos} />
        <KeywordColumn tone="neg" rows={kwNeg} />
      </div>
    </Block>
  );
}
