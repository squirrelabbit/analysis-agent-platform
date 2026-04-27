import type { Project } from "@/features/project/types/project";
import { useDatasetId } from "@/hooks/useDatasetId";
import { useOutletContext } from "react-router-dom";
import { useDatasetVersion } from "../hooks/useDatasetVersion";
import { useDatasetDetail } from "../hooks/useDataset";
import DatasetVersionHeader from "../components/version/DatasetVersionHeader";
import DatasetVersionHistory from "../components/version/DatasetVersionHistory";
import { VersionSkeleton } from "../components/version/VersionSkeleton";

export default function DatasetVersionPage() {
  const { project } = useOutletContext<{ project: Project }>();
  const { datasetId } = useDatasetId();
  const { data: dataset } = useDatasetDetail(project.id, datasetId);
  const { data: versions = [], isLoading } = useDatasetVersion(
    project.id,
    datasetId,
  );

  if (!project || !dataset) return null;
  return (
    <div className="p-8">
      <DatasetVersionHeader project={project} dataset={dataset} />
      {isLoading ? <VersionSkeleton /> : (
        <DatasetVersionHistory
          projectId={project.id}
          datasetId={datasetId}
          versions={versions}
        />
      )}
    </div>
  );
}
