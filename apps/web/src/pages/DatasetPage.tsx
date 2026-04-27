import DatasetHeader from "@/features/dataset/components/dataset/DatasetHeader";
import DatasetTable from "@/features/dataset/components/dataset/DatasetTable";
import { useDataset } from "@/features/dataset/hooks/useDataset";
import type { Project } from "@/features/project/types/project";
import { useOutletContext } from "react-router-dom";

export default function DatasetPage() {
  const { project } = useOutletContext<{project: Project}>()
  const { data: datasets = [] } = useDataset(project.id);
  
  return (
    <div className="p-8">
      {project && <DatasetHeader project={project} />}
      <div className="py-5">
        <DatasetTable datasets={datasets} />
      </div>
    </div>
  );
}
