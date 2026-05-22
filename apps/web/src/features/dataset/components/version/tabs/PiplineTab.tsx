import type { BuildStageResult, DatasetVersionDetail, Stage } from "@/features/dataset/types/datasetVersion";
import { BuildStageCard } from "./pipline/BuildStageCard";

export default function PiplineTab({ detail }: { detail: DatasetVersionDetail }) {
  const { id, clean, docGenuineness, clauseLabel } = detail;

  const stages: {stage: Stage, buildStage: BuildStageResult, summary?: any}[] = [
    {stage: 'clean', buildStage: clean, },
    {stage: 'docGenuineness', buildStage: docGenuineness },
    {stage: 'clauseLabel', buildStage: clauseLabel },
  ]

  return (
    <div className="flex flex-col gap-2">
      {stages.map((s, idx) => 
        
        <BuildStageCard key={idx} id={id} stage={s.stage} buildStage={s.buildStage}  />
      )}
    </div>
  );
}
