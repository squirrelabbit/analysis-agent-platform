import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { useDatasetId } from "@/hooks/useDatasetId";
import type { Project } from "@/features/project/types/project";
import { useOutletContext } from "react-router-dom";
import { useDatasetDetail } from "@/features/dataset/hooks/useDataset";

export default function PromptPage() {
  const { project } = useOutletContext<{ project: Project }>();
  const { datasetId } = useDatasetId();
  const { data: dataset } = useDatasetDetail(project.id, datasetId);

  if (!project || !dataset) return null;
  return (
    <div className="p-8">
      <div className="mb-3">
        <Breadcrumb>
          <BreadcrumbList>
            <BreadcrumbItem>
              <BreadcrumbLink href="/projects">프로젝트</BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink href={`/projects/${project.id}/datasets`}>
                {project.name}
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>{dataset.name}</BreadcrumbPage>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>
      </div>
      <h1>프롬프트 페이지</h1>
    </div>
  );
}
