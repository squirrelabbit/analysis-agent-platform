import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { useVersions } from "../hooks/version.query";
import { useDocGenuinenessCompare } from "../hooks/build.query";
import { GENUINENESS_LABELS } from "../constants/genuineness";

// 진성 분류 모델 비교 화면 (silverone 2026-06-15). 같은 원본을 다른 모델로 빌드한
// 두 버전을 골라 doc_id 1:1로 비교한다. 일치율 + 혼동행렬 + 불일치 문서 목록.

const tierLabel = (key: string) =>
  (GENUINENESS_LABELS as Record<string, string>)[key] ?? key;

// 버전 라벨 — 모델 표시명이 있으면 함께. (목록 select용)
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

  const [versionA, setVersionA] = useState("");
  const [versionB, setVersionB] = useState("");

  const { data, isLoading, isError, error } = useDocGenuinenessCompare(
    projectId,
    datasetId,
    versionA,
    versionB,
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
        같은 데이터를 서로 다른 모델로 빌드한 두 버전을 골라 문서 단위로 비교합니다.
        비교값은 사람 보정 전 원본 모델 라벨입니다.
      </p>

      {/* 버전 선택 */}
      <div className="mt-5 flex flex-wrap items-end gap-4">
        <div>
          <div className="mb-1 text-xs font-medium text-zinc-500">버전 A</div>
          <Select value={versionA} onValueChange={setVersionA}>
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
          <div className="mb-1 text-xs font-medium text-zinc-500">버전 B</div>
          <Select value={versionB} onValueChange={setVersionB}>
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
      </div>

      {ready.length < 2 && (
        <p className="mt-4 text-sm text-amber-600">
          비교하려면 진성 분류가 완료된 버전이 2개 이상 필요합니다. 같은 원본을 다른
          모델로 한 번 더 빌드하세요.
        </p>
      )}
      {versionA && versionB && versionA === versionB && (
        <p className="mt-4 text-sm text-amber-600">서로 다른 버전을 선택하세요.</p>
      )}

      {isLoading && <p className="mt-6 text-sm text-zinc-400">비교 중…</p>}
      {isError && (
        <p className="mt-6 text-sm text-red-500">
          비교에 실패했습니다: {(error as Error)?.message ?? "알 수 없는 오류"}
        </p>
      )}

      {data && (
        <div className="mt-6 space-y-6">
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
