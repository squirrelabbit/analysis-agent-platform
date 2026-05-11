import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import type { Project } from "@/features/project/types/project";
import type { Dataset } from "../../types/dataset";
import CreateDialog from "@/components/common/dialogs/CreateDialog";
import UploadVersionForm from "./forms/UploadVersionForm";
import { useUploadVersionMutation } from "../../hooks/useVersionMutation";
import { mapUploadFormToRequest } from "../../api/datasetVersion.mapper";

export default function DatasetVersionHeader({
  project,
  dataset,
}: {
  project: Project;
  dataset: Dataset;
}) {
  const { mutateAsync } = useUploadVersionMutation();
  return (
    <div>
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
      <div className="flex justify-between my-3">
        <h2 className="mb-1 text-xl font-bold text-[#16192b]">데이터셋 버전</h2>
        <CreateDialog title="데이터" formId="upload-dataset-version-form">
          {(close) => (
            <UploadVersionForm
              formId="upload-dataset-version-form"
              type={dataset.dataType}
              onSubmit={async (data) => {
                await mutateAsync({
                  projectId: project.id,
                  datasetId: dataset.id,
                  req: mapUploadFormToRequest(data),
                });
              }}
              onSuccess={close}
            />
          )}
        </CreateDialog>
      </div>
    </div>
  );
}
