import type { Project } from "@/features/project/types/project";
import { useDatasetId } from "@/hooks/useDatasetId";
import { useOutletContext } from "react-router-dom";
import {
  useDatasetVersion,
  useDatasetVersionDetail,
} from "../hooks/useDatasetVersion";
import { useDatasetDetail } from "../hooks/useDataset";
import { VersionSkeleton } from "../components/version/VersionSkeleton";
import DatasetVersionHeader from "../components/version/DatasetVersionHeader";
import { useEffect, useState } from "react";
import { DatasetVersionDetail } from "../components/version/DatasetVersionDetail";
import DatasetVersionItem from "../components/version/DatasetVersionItem";

export default function DatasetVersionPage() {
  const { project } = useOutletContext<{ project: Project }>();
  const { datasetId } = useDatasetId();
  const { data: dataset } = useDatasetDetail(project.id, datasetId);
  const { data: versions = [], isLoading } = useDatasetVersion(
    project.id,
    datasetId,
  );

  const [selected, setSelected] = useState<string>();

  const { data: version } = useDatasetVersionDetail(
    project.id,
    datasetId,
    selected,
  );

  useEffect(() => {
  if (isLoading) return

  setSelected(
    versions.find((v) => v.isActive)?.id ??
      versions[0]?.id,
  )
}, [versions])

  if (!project || !dataset) return null;
  return (
    <>
      <div className="flex h-[calc(100vh-56px)]">
        <aside className="w-80  border-r bg-white flex flex-col p-4">
          <DatasetVersionHeader project={project} dataset={dataset} />
          {isLoading ? (
            <VersionSkeleton />
          ) : (
            <div className="flex flex-col gap-2">
              {versions.map((v) => (
                <DatasetVersionItem
                  key={v.id}
                  version={v}
                  isSelected={selected === v.id}
                  onSelect={() => setSelected(v.id)}
                />
              ))}
            </div>
          )}
        </aside>
        <div className="flex-1 overflow-x-auto">
          {version && <DatasetVersionDetail version={version} />}
        </div>
      </div>
    </>
  );
}
