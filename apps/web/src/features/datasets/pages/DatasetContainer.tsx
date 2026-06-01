import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useDatasets } from "../hooks/dataset.query";
import DatasetHeader from "../components/DatasetHeader";
import DatasetItem from "../components/DatasetItem";

export default function DatasetContainer() {
  const { projectId } = useProjectParams();
  const { data: project } = useProjectDetail(projectId);
  const { data: datasets = [] } = useDatasets();

  if (!project) return null;
  return (
    <div className="p-8 flex flex-col gap-4">
      <DatasetHeader {...project} />
      <div className="grid grid-cols-2 gap-3">
        {datasets.map((dataset) => <DatasetItem key={dataset.id} {...dataset} />)}
      </div>
    </div>
  );
}
