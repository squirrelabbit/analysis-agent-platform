import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import type { Project } from "@/features/projects/models/model";
import type { Dataset } from "../models/model";

export default function HeadBreadcrumb({
  project,
  dataset,
}: {
  project: Project;
  dataset?: Dataset;
}) {
  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink href="/projects">프로젝트</BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        <BreadcrumbItem>
          {dataset ? (
            <BreadcrumbLink href={`/projects/${project.id}`}>
              {project.name}
            </BreadcrumbLink>
          ) : (
            <BreadcrumbPage>{project.name}</BreadcrumbPage>
          )}
        </BreadcrumbItem>
        {dataset && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>{dataset.name}</BreadcrumbPage>
            </BreadcrumbItem>
          </>
        )}
      </BreadcrumbList>
    </Breadcrumb>
  );
}
