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
import UploadVersionForm from "./UploadVersionForm";
import { mapUploadFormToRequest } from "../../api/datasetVersion.mapper";
import { useDatasetId } from "@/hooks/useDatasetId";
import { useUploadVersionMutation } from "../../hooks/useVersionMutation";

export default function DatasetVersionHeader({
  project,
  dataset,
}: {
  project: Project;
  dataset: Dataset;
}) {
  const { datasetId } = useDatasetId();
  const upload = useUploadVersionMutation()
  return (
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
      <header className="flex justify-between items-center pt-4">
        <div>
          <h2 className="mb-1 text-xl font-bold text-[#16192b]">
            데이터셋 버전
          </h2>
          <p className="text-xs text-[#9399b0]">
            버전별 데이터를 관리합니다. 하나의 버전만 활성화되며 활성 버전으로
            분석이 실행됩니다.
          </p>
        </div>
        <div className="flex gap-2">
          <CreateDialog title="데이터" formId="upload-dataset-form">
            {(close) => (
              <UploadVersionForm
                formId="upload-dataset-form"
                type={dataset.dataType}
                onSubmit={async (data) => {
                  await upload.mutateAsync({
                    projectId: project.id,
                    datasetId: datasetId,
                    req: mapUploadFormToRequest(data),
                  })
                }}
                onSuccess={close}
              />
            )}
          </CreateDialog>
        </div>
      </header>
    </div>
  );
}
