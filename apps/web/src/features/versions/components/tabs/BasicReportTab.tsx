import { useBasicAnalysis } from "../../hooks/build.query";
import { BuildTabLoading, BuildRefreshButton } from "../BuildStatusMeta";
import BasicReportLayout, { UnitBadge } from "../BasicReportLayout";
import type { ReportBlock } from "../../models/basicReport";

// 기초분석보고서 탭 — 백엔드(useBasicAnalysis) 응답을 기본 템플릿
// (report_basic_template.sample.md) 공통 디자인으로 렌더한다. 패널 렌더는
// BasicReportLayout(보고서 에디터와 공용)으로 위임해 동일하게 보이도록 한다.

export function BasicReportTab() {
  const { data, isLoading, error } = useBasicAnalysis();

  if (isLoading) return <BuildTabLoading />;

  // 가장 흔한 에러: clean이 아직 준비 안 됨(400 clean_not_ready).
  const errMsg = error
    ? ((error as { response?: { data?: { detail?: string } } })?.response?.data
        ?.detail ?? "기초분석보고서를 불러오지 못했습니다.")
    : null;

  return (
    <div className="space-y-4">
      {/* 탭별 새로고침 — 전처리 재실행 후 이 탭에 머무른 채로 갱신할 수 있게. */}
      <div className="flex justify-end">
        <BuildRefreshButton />
      </div>

      {errMsg ? (
        <div className="rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800">
          {errMsg === "clean_not_ready"
            ? "정제(clean)가 완료되면 기초분석보고서가 표시됩니다."
            : errMsg}
        </div>
      ) : !data || data.blocks.length === 0 ? (
        <div className="rounded-2xl border border-zinc-100 bg-white px-5 py-8 text-center text-sm text-zinc-500 shadow-sm">
          표시할 분석 블록이 없습니다.
        </div>
      ) : (
        <div className="space-y-5">
          {data.missing_sections.length > 0 && (
            <div className="rounded-xl border border-zinc-100 bg-zinc-50 px-4 py-2.5 text-xs text-zinc-500">
              빌드 미완으로 일부 섹션이 제외됨:{" "}
              {data.missing_sections.map((m) => m.section_id).join(", ")}
            </div>
          )}
          {data.blocks.map((block) => (
            <BlockCard key={block.block_id ?? block.section_id} block={block} />
          ))}
        </div>
      )}
    </div>
  );
}

// ── 카드 ─────────────────────────────────────────────────────

function BlockCard({ block }: { block: ReportBlock }) {
  const unit =
    block.unit_basis === "doc" || block.unit_basis === "clause"
      ? block.unit_basis
      : null;
  return (
    <div className="rounded-2xl border border-zinc-100 bg-white p-5.5 shadow-sm">
      <div className="mb-4 flex items-start gap-3">
        <div className="text-[15px] font-bold text-zinc-900">{block.title}</div>
        {/* 최신년도만 집계된 섹션(개요 제외) 표시 — silverone 2026-06-25 */}
        {block.scope_label && (
          <span className="rounded-md bg-amber-50 px-2 py-0.5 text-[11px] font-semibold text-amber-700">
            {block.scope_label}
          </span>
        )}
        {unit && <UnitBadge unit={unit} />}
      </div>
      <BasicReportLayout layout={block.layout} />
    </div>
  );
}
