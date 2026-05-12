import type { DatasetVersion } from "@/features/dataset/types/datasetVersion";
import { BuildStageCard } from "./pipline/BuildStageCard";

export default function PiplineTab({ version }: { version: DatasetVersion }) {
  const { buildStages, projectId, datasetId, id } = version;
  const routeParams = {
    projectId: projectId,
    datasetId: datasetId,
    versionId: id,
  };

  return (
    <div className="flex flex-col gap-2">
      {buildStages.map((bs, idx) => (
        <BuildStageCard key={idx} buildStage={bs} routeParams={routeParams} />
      ))}
    </div>
  );
}
