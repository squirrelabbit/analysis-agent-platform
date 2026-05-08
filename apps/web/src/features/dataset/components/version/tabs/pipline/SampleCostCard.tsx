export interface SampleCostEstimate {
  sampleCost: number;
  totalEstimated: number;
  costPerRow: number;
  estimatedMinutes: number;
  totalRows: number;
}

interface Props {
  cost: SampleCostEstimate;
  sampleSize: number;
}

function CostSeg({
  label, value, sub,
}: { label: string; value: string; sub: string }) {
  return (
    <div className="flex-1 text-center px-3 py-2.5
                    border-r border-amber-200/60 last:border-r-0">
      <p className="text-[10px] font-mono text-amber-600/70
                    uppercase tracking-wide mb-1">
        {label}
      </p>
      <p className="text-[15px] font-bold font-mono text-amber-700 leading-none">
        {value}
      </p>
      <p className="text-[9px] text-amber-600/60 mt-1">{sub}</p>
    </div>
  );
}

export function SampleCostCard({ cost, sampleSize }: Props) {
  return (
    <div className="flex border border-amber-200 rounded-lg overflow-hidden
                    bg-amber-50">
      <CostSeg
        label="샘플 비용"
        value={`$${cost.sampleCost.toFixed(4)}`}
        sub={`${sampleSize.toLocaleString("ko-KR")}건 기준`}
      />
      <CostSeg
        label="전체 예상"
        value={`$${cost.totalEstimated.toFixed(4)}`}
        sub={`${cost.totalRows.toLocaleString("ko-KR")}건 추산`}
      />
      <CostSeg
        label="건당 단가"
        value={`$${cost.costPerRow.toFixed(6)}`}
        sub="평균"
      />
      <CostSeg
        label="예상 소요"
        value={`~${cost.estimatedMinutes}m`}
        sub="전체 기준"
      />
    </div>
  );
}