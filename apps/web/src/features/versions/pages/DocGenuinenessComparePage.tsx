import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { useVersions } from "../hooks/version.query";
import { useDocGenuinenessCompare, useDocGenuinenessRuns } from "../hooks/build.query";
import { GENUINENESS_LABELS } from "../constants/genuineness";

// 진성 분류 모델 비교 화면 (silverone 2026-06-15). 버전 1개를 고르면 그 버전에
// 모델별로 누적된 결과(run) 중 두 모델을 골라 doc_id 1:1로 비교한다.
// 일치율 + 혼동행렬 + 불일치 문서 목록.

const tierLabel = (key: string) =>
  (GENUINENESS_LABELS as Record<string, string>)[key] ?? key;

function versionOption(v: { id: string; createdAt: string }) {
  return `${v.id.slice(0, 8)} · ${v.createdAt.slice(0, 16).replace("T", " ")}`;
}

export default function DocGenuinenessComparePage() {
  const { projectId, datasetId } = useDatasetParams();
  const navigate = useNavigate();
  const { data: versions = [] } = useVersions();

  // 진성 분류가 끝난 버전만 비교 대상.
  const ready = useMemo(
    () => versions.filter((v) => v.docGenuinenessStatus === "completed" || v.docGenuinenessStatus === "ready"),
    [versions],
  );

  const [versionId, setVersionId] = useState("");
  const [modelA, setModelA] = useState("");
  const [modelB, setModelB] = useState("");

  const { data: runs = [] } = useDocGenuinenessRuns(projectId, datasetId, versionId);

  // 버전이 바뀌면 모델 선택 초기화.
  const onVersionChange = (v: string) => {
    setVersionId(v);
    setModelA("");
    setModelB("");
  };

  const { data, isLoading, isError, error } = useDocGenuinenessCompare(
    projectId,
    datasetId,
    versionId,
    modelA,
    modelB,
  );

  const pct = data ? Math.round(data.agreement_rate * 1000) / 10 : 0;

  return (
    <div className="mx-auto max-w-5xl px-6 py-6">
      <button
        onClick={() => navigate(-1)}
        className="mb-4 inline-flex items-center gap-1 text-sm text-zinc-500 hover:text-zinc-800"
      >
        <ArrowLeft className="h-4 w-4" /> 버전 목록
      </button>
      <h1 className="text-xl font-bold text-zinc-800">진성 분류 모델 비교</h1>
      <p className="mt-1 text-sm text-zinc-500">
        버전을 고른 뒤, 그 버전에서 모델별로 돌린 결과 중 두 모델을 골라 문서 단위로
        비교합니다. 비교값은 사람 보정 전 원본 모델 라벨입니다.
      </p>

      {/* 버전 + 모델 선택 */}
      <div className="mt-5 flex flex-wrap items-end gap-4">
        <div>
          <div className="mb-1 text-xs font-medium text-zinc-500">버전</div>
          <Select value={versionId} onValueChange={onVersionChange}>
            <SelectTrigger className="w-64 text-xs">
              <SelectValue placeholder="버전 선택" />
            </SelectTrigger>
            <SelectContent>
              {ready.map((v) => (
                <SelectItem key={v.id} value={v.id} className="text-xs">
                  {versionOption(v)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div>
          <div className="mb-1 text-xs font-medium text-zinc-500">모델 A</div>
          <Select value={modelA} onValueChange={setModelA} disabled={!versionId}>
            <SelectTrigger className="w-56 text-xs">
              <SelectValue placeholder="모델 선택" />
            </SelectTrigger>
            <SelectContent>
              {runs.map((r) => (
                <SelectItem key={r.model} value={r.model} className="text-xs">
                  {r.model_display_name || r.model}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div>
          <div className="mb-1 text-xs font-medium text-zinc-500">모델 B</div>
          <Select value={modelB} onValueChange={setModelB} disabled={!versionId}>
            <SelectTrigger className="w-56 text-xs">
              <SelectValue placeholder="모델 선택" />
            </SelectTrigger>
            <SelectContent>
              {runs.map((r) => (
                <SelectItem key={r.model} value={r.model} className="text-xs">
                  {r.model_display_name || r.model}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {ready.length === 0 && (
        <p className="mt-4 text-sm text-amber-600">
          진성 분류가 완료된 버전이 없습니다.
        </p>
      )}
      {versionId && runs.length < 2 && (
        <p className="mt-4 text-sm text-amber-600">
          이 버전은 모델 결과가 {runs.length}개뿐입니다. 같은 버전을 다른 모델로 한 번
          더 빌드하면 비교할 수 있습니다.
        </p>
      )}
      {modelA && modelB && modelA === modelB && (
        <p className="mt-4 text-sm text-amber-600">서로 다른 모델을 선택하세요.</p>
      )}

      {isLoading && <p className="mt-6 text-sm text-zinc-400">비교 중…</p>}
      {isError && (
        <p className="mt-6 text-sm text-red-500">
          비교에 실패했습니다: {(error as Error)?.message ?? "알 수 없는 오류"}
        </p>
      )}

      {data && (
        <div className="mt-6 space-y-6">
          {/* 결론 카드 — 정답 판정이 아니라 합의/불일치 기반 판정 보조. */}
          {(() => {
            const aName = data.version_a.model_display_name || data.version_a.model || "모델 A";
            const bName = data.version_b.model_display_name || data.version_b.model || "모델 B";
            const oe = data.override_eval;
            const accent =
              data.verdict_level === "ground_truth"
                ? "border-emerald-200 bg-emerald-50/60"
                : data.verdict_level === "review_needed"
                  ? "border-amber-200 bg-amber-50/60"
                  : "border-zinc-200 bg-zinc-50/60";
            return (
              <div className={`rounded-2xl border p-5 ${accent}`}>
                <div className="text-sm font-bold text-zinc-800">결론</div>
                <p className="mt-2 text-sm text-zinc-700">
                  총 {data.compared.toLocaleString()}건 중 {data.matched.toLocaleString()}건 일치 (일치율 {pct}%).
                </p>
                {data.verdict_level === "ground_truth" && oe && (
                  <p className="mt-1 text-sm text-zinc-700">
                    사람 보정 정답 {oe.sample_count}건 기준 — {aName} {Math.round(oe.a_accuracy * 100)}% ({oe.a_correct}건),{" "}
                    {bName} {Math.round(oe.b_accuracy * 100)}% ({oe.b_correct}건).{" "}
                    {oe.leader === "tie" ? (
                      <b>두 모델 정확도 동률.</b>
                    ) : (
                      <b>{oe.leader === "a" ? aName : bName} 모델이 정답에 더 가까움.</b>
                    )}
                  </p>
                )}
                {data.verdict_level === "agreement_only" && (
                  <p className="mt-1 text-sm text-zinc-700">
                    일치율은 높지만 <b>정답 데이터가 없어 어느 모델이 맞는지는 판단할 수 없습니다.</b> 표본 보정(불일치 문서에 정답 지정) 후 재확인하세요.
                  </p>
                )}
                {data.verdict_level === "review_needed" && (
                  <p className="mt-1 text-sm text-amber-700">
                    일치율이 낮아 모델 간 판단 기준 차이가 큽니다. <b>운영 적용 전 불일치 문서 검토가 필요합니다.</b>
                  </p>
                )}
                {data.patterns.length > 0 && (
                  <p className="mt-1 text-xs text-zinc-500">
                    주요 불일치 패턴:{" "}
                    {data.patterns.slice(0, 3).map((p, i) => (
                      <span key={`${p.a_genuineness}-${p.b_genuineness}`}>
                        {i > 0 && ", "}
                        {aName.slice(0, 6)}={tierLabel(p.a_genuineness)} / {bName.slice(0, 6)}=
                        {tierLabel(p.b_genuineness)} {p.count}건
                      </span>
                    ))}
                  </p>
                )}
                {/* 추천 액션 */}
                <div className="mt-3 flex flex-wrap gap-2 text-xs">
                  {data.unreviewed_disagreements > 0 && (
                    <span className="rounded-full bg-white px-2.5 py-1 font-medium text-zinc-600 ring-1 ring-zinc-200">
                      검토 필요 {data.unreviewed_disagreements.toLocaleString()}건 (정답 미보정 불일치)
                    </span>
                  )}
                  {data.verdict_level === "ground_truth" && oe && oe.leader !== "tie" && (
                    <span className="rounded-full bg-white px-2.5 py-1 font-medium text-emerald-700 ring-1 ring-emerald-200">
                      추천: {oe.leader === "a" ? bName : aName} 적용 보류, {oe.leader === "a" ? aName : bName} 우선
                    </span>
                  )}
                  {data.verdict_level === "agreement_only" && (
                    <span className="rounded-full bg-white px-2.5 py-1 font-medium text-zinc-600 ring-1 ring-zinc-200">
                      추천: 정답 샘플 확보 후 재평가
                    </span>
                  )}
                  {data.verdict_level === "review_needed" && (
                    <span className="rounded-full bg-white px-2.5 py-1 font-medium text-amber-700 ring-1 ring-amber-200">
                      추천: 불일치 {data.disagreements_total.toLocaleString()}건 검토 후 적용 결정
                    </span>
                  )}
                </div>
              </div>
            );
          })()}

          {/* 요약 */}
          <div className="rounded-2xl border border-zinc-200 p-5">
            <div className="flex flex-wrap items-center gap-6">
              <div>
                <div className="text-3xl font-extrabold text-zinc-800">{pct}%</div>
                <div className="text-xs text-zinc-500">
                  일치율 ({data.matched.toLocaleString()} / {data.compared.toLocaleString()} 문서)
                </div>
              </div>
              <div className="text-xs text-zinc-600">
                <div>
                  A:{" "}
                  <b>{data.version_a.model_display_name || data.version_a.model || "모델 미상"}</b>{" "}
                  ({data.version_a.total.toLocaleString()})
                </div>
                <div>
                  B:{" "}
                  <b>{data.version_b.model_display_name || data.version_b.model || "모델 미상"}</b>{" "}
                  ({data.version_b.total.toLocaleString()})
                </div>
              </div>
              {(data.only_in_a > 0 || data.only_in_b > 0) && (
                <div className="text-xs text-amber-600">
                  한쪽에만 있는 문서: A {data.only_in_a} · B {data.only_in_b}
                  <div className="text-[11px] text-amber-500">
                    (두 버전 원본이 다를 수 있음)
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* 혼동행렬 */}
          <div className="rounded-2xl border border-zinc-200 p-5">
            <div className="mb-3 text-sm font-bold text-zinc-700">
              혼동행렬 (행: A 라벨, 열: B 라벨)
            </div>
            <table className="text-xs">
              <thead>
                <tr>
                  <th className="px-2 py-1 text-left text-zinc-400">A \ B</th>
                  {data.tiers.map((t) => (
                    <th key={t} className="px-3 py-1 text-zinc-500">
                      {tierLabel(t)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.tiers.map((rowTier, i) => (
                  <tr key={rowTier}>
                    <td className="px-2 py-1 font-medium text-zinc-500">{tierLabel(rowTier)}</td>
                    {data.tiers.map((colTier, j) => {
                      const n = data.confusion[i]?.[j] ?? 0;
                      const diag = i === j;
                      return (
                        <td
                          key={colTier}
                          className={`px-3 py-1 text-center tabular-nums ${
                            n === 0
                              ? "text-zinc-300"
                              : diag
                                ? "font-bold text-emerald-600"
                                : "font-semibold text-red-500"
                          }`}
                        >
                          {n}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-2 text-[11px] text-zinc-400">
              대각선(초록)은 두 모델이 같게 분류한 문서, 그 외(빨강)는 불일치입니다.
            </p>
          </div>

          {/* 불일치 목록 */}
          <div className="rounded-2xl border border-zinc-200 p-5">
            <div className="mb-3 text-sm font-bold text-zinc-700">
              불일치 문서 {data.disagreements_total.toLocaleString()}건
            </div>
            {data.disagreements.length === 0 ? (
              <p className="text-sm text-zinc-400">불일치 문서가 없습니다.</p>
            ) : (
              <div className="space-y-3">
                {data.disagreements.map((d) => (
                  <div key={d.doc_id} className="rounded-lg border border-zinc-100 p-3">
                    <div className="flex flex-wrap items-center gap-2 text-xs">
                      <span className="rounded bg-zinc-100 px-1.5 py-0.5 font-mono text-zinc-500">
                        {d.doc_id}
                      </span>
                      <span className="font-semibold text-zinc-600">
                        A: {tierLabel(d.a_genuineness)}
                      </span>
                      <span className="text-zinc-300">vs</span>
                      <span className="font-semibold text-zinc-600">
                        B: {tierLabel(d.b_genuineness)}
                      </span>
                      {d.override_genuineness && (
                        <span className="rounded bg-violet-50 px-1.5 py-0.5 font-semibold text-violet-600">
                          정답(보정): {tierLabel(d.override_genuineness)}
                        </span>
                      )}
                    </div>
                    {d.cleaned_text && (
                      <p className="mt-2 line-clamp-3 text-xs text-zinc-600">{d.cleaned_text}</p>
                    )}
                  </div>
                ))}
              </div>
            )}
            {data.disagreements_total > data.disagreements.length && (
              <p className="mt-3 text-[11px] text-zinc-400">
                상위 {data.disagreements.length}건만 표시 (전체 {data.disagreements_total}건).
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
