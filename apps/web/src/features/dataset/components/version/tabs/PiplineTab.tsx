import type { DatasetVersion } from "@/features/dataset/types/datasetVersion";
import { BuildStageCard } from "./pipline/BuildStageCard";

export default function PiplineTab ({ version }: { version: DatasetVersion }) {
  const { buildStages } = version
  return (
    <div className="flex flex-col gap-2">
      {buildStages.map((bs, idx) => 
        <BuildStageCard key={idx} buildStage={bs} />
      )}
    </div>
  )
}