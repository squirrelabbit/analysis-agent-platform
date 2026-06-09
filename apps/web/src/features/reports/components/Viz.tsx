// 보고서 블록 viz 렌더러. 보관함 LibraryItem.viz(key) → 해당 컴포넌트로 분기.
// 보고서 에디터 HTML의 순수 CSS/SVG viz를 React로 이식. 색은 앱 팔레트(editor.ts) 사용.
import {
  SENT_LABEL,
  VIZ_COLOR,
  type BarsData,
  type DivergeData,
  type DonutData,
  type EvidenceData,
  type GridData,
  type LibraryItem,
  type LineData,
  type MetricData,
} from "../models/editor";
import { ArrowRight, ChevronUp } from "lucide-react";

function VizBars({ d }: { d: BarsData }) {
  const max = Math.max(...d.rows.map((r) => r.v));
  return (
    <div className="flex flex-col gap-2.5">
      {d.rows.map((r) => (
        <div
          key={r.k}
          className="grid items-center gap-3"
          style={{ gridTemplateColumns: "92px 1fr 46px" }}
        >
          <span className="truncate text-right text-[12.5px] font-semibold text-zinc-600">
            {r.k}
          </span>
          <div className="h-5.5 overflow-hidden rounded-md bg-zinc-100">
            <div
              className="h-full rounded-md bg-linear-to-r from-violet-600 to-violet-500"
              style={{ width: `${((r.v / max) * 100).toFixed(1)}%` }}
            />
          </div>
          <span className="text-right text-[13px] font-extrabold tabular-nums text-zinc-800">
            {r.v.toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  );
}

function VizDiverge({ d }: { d: DivergeData }) {
  const max = Math.max(...d.rows.map((r) => Math.abs(r.v)));
  return (
    <>
      <div className="flex flex-col gap-2.5">
        {d.rows.map((r) => {
          const pos = r.v >= 0;
          const w = `${((Math.abs(r.v) / max) * 100).toFixed(1)}%`;
          return (
            <div
              key={r.k}
              className="grid items-center gap-3"
              style={{ gridTemplateColumns: "96px 1fr 64px" }}
            >
              <span className="truncate text-right text-[12.5px] font-semibold text-zinc-600">
                {r.k}
              </span>
              <div className="relative h-6">
                {/* 중앙 0 기준선 */}
                <span className="absolute left-1/2 top-0 h-full w-px -translate-x-1/2 bg-zinc-300" />
                <div
                  className={
                    pos
                      ? "absolute left-1/2 right-0 flex h-full items-center justify-start"
                      : "absolute left-0 right-1/2 flex h-full items-center justify-end"
                  }
                >
                  <div
                    className={
                      pos
                        ? "h-5.5 rounded-r-md bg-emerald-500"
                        : "h-5.5 rounded-l-md bg-red-500"
                    }
                    style={{ width: w, minWidth: 2 }}
                  />
                </div>
              </div>
              <span
                className={`text-right text-[12.5px] font-extrabold tabular-nums ${
                  pos ? "text-emerald-600" : "text-red-600"
                }`}
              >
                {pos ? "+" : "−"}
                {Math.abs(r.v)}
                {d.unit ?? ""}
              </span>
            </div>
          );
        })}
      </div>
      <div className="mt-3 flex justify-center gap-5 text-[11.5px] font-semibold text-zinc-600">
        <span className="inline-flex items-center gap-1.5">
          <i className="h-2.5 w-2.5 rounded-sm bg-red-500" />
          감소
        </span>
        <span className="inline-flex items-center gap-1.5">
          <i className="h-2.5 w-2.5 rounded-sm bg-emerald-500" />
          증가
        </span>
      </div>
    </>
  );
}

function VizDonut({ d }: { d: DonutData }) {
  let acc = 0;
  const stops = d.rows
    .map((r) => {
      const s = acc;
      acc += r.pct;
      return `${r.color} ${s}% ${acc}%`;
    })
    .join(", ");
  return (
    <div className="flex items-center gap-6.5">
      <div
        className="h-32.5 w-32.5 shrink-0 rounded-full"
        style={{
          background: `conic-gradient(${stops})`,
          WebkitMaskImage: "radial-gradient(circle, transparent 54%, #000 55%)",
          maskImage: "radial-gradient(circle, transparent 54%, #000 55%)",
        }}
      />
      <div className="flex flex-1 flex-col gap-2.75">
        {d.rows.map((r) => (
          <div key={r.k} className="flex items-center gap-2.5 text-[13px]">
            <i
              className="h-2.75 w-2.75 shrink-0 rounded-[3px]"
              style={{ background: r.color }}
            />
            <span className="font-semibold text-zinc-600">{r.k}</span>
            <span className="ml-auto font-extrabold tabular-nums text-zinc-800">
              {r.pct}%
            </span>
            <span className="min-w-12.5 text-right text-xs font-semibold text-zinc-400">
              {r.n}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function VizLine({ d }: { d: LineData }) {
  const W = 620,
    H = 250,
    padL = 46,
    padR = 16,
    padT = 24,
    padB = 34;
  const xs = d.pts.length;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;
  const step = xs > 1 ? innerW / (xs - 1) : 0;
  const X = (i: number) => padL + i * step;
  const Y = (v: number) => padT + innerH - (v / d.max) * innerH;
  const poly = d.pts.map((p, i) => `${X(i).toFixed(0)},${Y(p.v).toFixed(0)}`).join(" ");
  const area =
    `M${X(0).toFixed(0)} ${Y(d.pts[0].v).toFixed(0)} ` +
    d.pts.map((p, i) => `L${X(i).toFixed(0)} ${Y(p.v).toFixed(0)}`).join(" ") +
    ` L${X(xs - 1).toFixed(0)} ${padT + innerH} L${padL} ${padT + innerH} Z`;
  return (
    <div className="w-full">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet" className="block h-auto w-full">
        <line x1={padL} y1={padT} x2={padL} y2={padT + innerH} stroke="#e4e4e7" />
        <line x1={padL} y1={padT + innerH} x2={W - padR} y2={padT + innerH} stroke="#e4e4e7" />
        {[0, 0.5, 1].map((f) => {
          const y = padT + innerH - f * innerH;
          return (
            <g key={f}>
              <line
                x1={padL}
                y1={y}
                x2={W - padR}
                y2={y}
                stroke="#f4f4f5"
                strokeWidth={1}
                strokeDasharray="3 4"
              />
              <text x={padL - 6} y={y + 4} textAnchor="end" fontSize={11} fill="#a1a1aa">
                {Math.round(f * d.max)}
              </text>
            </g>
          );
        })}
        {d.refIdx != null && (
          <>
            <line
              x1={X(d.refIdx)}
              y1={padT}
              x2={X(d.refIdx)}
              y2={padT + innerH}
              stroke="#f59e0b"
              strokeWidth={1.5}
              strokeDasharray="4 4"
            />
            <text
              x={X(d.refIdx)}
              y={padT - 6}
              textAnchor="middle"
              fontSize={10.5}
              fontWeight={700}
              fill="#f59e0b"
            >
              {d.refLabel ?? ""}
            </text>
          </>
        )}
        <path d={area} fill={VIZ_COLOR.blue} fillOpacity={0.08} />
        <polyline
          points={poly}
          fill="none"
          stroke={VIZ_COLOR.blue}
          strokeWidth={2.5}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <g fill="#fff" stroke={VIZ_COLOR.blue} strokeWidth={2.5}>
          {d.pts.map((p, i) => (
            <circle key={i} cx={X(i)} cy={Y(p.v)} r={p.mark ? 4.5 : 3.5} />
          ))}
        </g>
        {d.pts.map((p, i) => (
          <text
            key={i}
            x={X(i)}
            y={H - 12}
            textAnchor="middle"
            fontSize={11}
            fill={p.mark ? "#52525b" : "#a1a1aa"}
            fontWeight={p.mark ? 700 : 400}
          >
            {p.x}
          </text>
        ))}
      </svg>
    </div>
  );
}

function VizMetric({ d }: { d: MetricData }) {
  return (
    <div className="flex items-stretch gap-3.5">
      <div className="flex-1 rounded-xl border border-zinc-100 px-4.5 py-4">
        <div className="text-xs font-semibold text-zinc-400">{d.beforeK}</div>
        <div className="mt-1.5 text-3xl font-extrabold leading-none tracking-tight tabular-nums text-zinc-900">
          {d.before}
          <span className="ml-0.5 text-[15px] font-bold text-zinc-400">{d.unit}</span>
        </div>
      </div>
      <div className="grid place-items-center text-zinc-300">
        <ArrowRight className="h-5.5 w-5.5" />
      </div>
      <div className="flex-1 rounded-xl border border-emerald-200 bg-emerald-50/70 px-4.5 py-4">
        <div className="text-xs font-semibold text-zinc-400">{d.afterK}</div>
        <div className="mt-1.5 text-3xl font-extrabold leading-none tracking-tight tabular-nums text-zinc-900">
          {d.after}
          <span className="ml-0.5 text-[15px] font-bold text-zinc-400">{d.unit}</span>
        </div>
        <div className="mt-2.5 inline-flex items-center gap-1.5 rounded-full border border-emerald-200 bg-white px-2.5 py-1 text-[13px] font-extrabold text-emerald-600">
          <ChevronUp className="h-3.25 w-3.25" strokeWidth={2.5} />
          {d.delta}
        </div>
      </div>
    </div>
  );
}

function VizEvidence({ d }: { d: EvidenceData }) {
  const bar: Record<string, string> = {
    pos: "before:bg-emerald-500",
    neg: "before:bg-red-500",
    neu: "before:bg-zinc-400",
  };
  const sb: Record<string, string> = {
    pos: "bg-emerald-50 text-emerald-600",
    neg: "bg-red-50 text-red-600",
    neu: "bg-zinc-100 text-zinc-500",
  };
  return (
    <div className="flex flex-col gap-2.5">
      {d.rows.map((r, i) => (
        <div
          key={i}
          className={`relative rounded-xl border border-zinc-100 px-3.75 py-3.25 before:absolute before:left-0 before:top-3.25 before:bottom-3.25 before:w-0.75 before:rounded-r-[3px] ${bar[r.s]}`}
        >
          <div className="pl-2 text-[13.5px] leading-relaxed text-zinc-900">
            {r.q}
          </div>
          <div className="mt-2.5 flex flex-wrap items-center gap-2 pl-2">
            <span className="rounded-full bg-zinc-100 px-2.25 py-0.75 text-[11.5px] font-semibold text-zinc-600">
              {r.aspect}
            </span>
            <span
              className={`rounded-full px-2.25 py-0.75 text-[11.5px] font-bold ${sb[r.s]}`}
            >
              {SENT_LABEL[r.s]}
            </span>
            <span className="ml-auto font-mono text-[11px] font-semibold text-zinc-400">
              {r.doc}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

export function VizGrid({ d }: { d: GridData }) {
  return (
    <table className="w-full border-collapse text-[12.5px]">
      <thead>
        <tr>
          {d.cols.map((c, i) => (
            <th
              key={i}
              className={`border-b border-zinc-300 px-3 py-2 text-[11.5px] font-bold whitespace-nowrap text-zinc-400 ${
                i ? "text-right" : "text-left"
              }`}
            >
              {c}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {d.rows.map((row, ri) => (
          <tr key={ri}>
            {row.map((cell, ci) => (
              <td
                key={ci}
                className={`border-b border-zinc-100 px-3 py-2.25 last:border-0 ${
                  ci
                    ? "text-right tabular-nums text-zinc-600"
                    : "font-semibold text-zinc-900"
                }`}
              >
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function VizPlan({ steps }: { steps: string[] }) {
  return (
    <div className="text-[13px] leading-relaxed text-zinc-600">
      {steps.map((s, i) => (
        <div key={i} className="flex gap-2.5 py-1.25">
          <b className="shrink-0 font-bold text-violet-700">{i + 1}.</b>
          <span>{s}</span>
        </div>
      ))}
    </div>
  );
}

/** LibraryItem.viz key → 해당 viz 컴포넌트로 분기 렌더 */
export function Viz({ lib }: { lib: LibraryItem }) {
  switch (lib.viz) {
    case "bars":
      return <VizBars d={lib.data as BarsData} />;
    case "diverge":
      return <VizDiverge d={lib.data as DivergeData} />;
    case "donut":
      return <VizDonut d={lib.data as DonutData} />;
    case "line":
      return <VizLine d={lib.data as LineData} />;
    case "metric":
      return <VizMetric d={lib.data as MetricData} />;
    case "evidence":
      return <VizEvidence d={lib.data as EvidenceData} />;
    case "grid":
      return <VizGrid d={lib.data as GridData} />;
    default:
      return null;
  }
}
