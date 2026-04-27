import CreateDialog from "@/components/common/dialogs/CreateDialog";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import type { Project } from "@/features/project/types/project";
import CreateDatasetForm from "./CreateDatasetForm";
import { useCreateDatasetMutation } from "../../hooks/useDatasetsMutation";
import { mapDatasetFormToRequest } from "../../api/dataset.mapper";
export default function DatasetHeader({ project }: { project: Project }) {
  const create = useCreateDatasetMutation();
  return (
    <div>
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink href="/projects">프로젝트</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{project.name}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
      <header className="flex justify-between pt-4">
        <div>
          <h2 className="mb-1 text-xl font-bold text-[#16192b]">
            데이터셋 관리
          </h2>
          <p className="text-sm text-[#9399b0]">
            이 프로젝트에서 사용할 데이터셋을 등록·관리합니다. 데이터셋별로
            프롬프트와 업로드 데이터가 연결됩니다.
          </p>
        </div>
        <CreateDialog title="데이터셋" formId="dataset-form">
          {(close) => (
            <CreateDatasetForm
              formId="dataset-form"
              onSubmit={async (data) => {
                await create.mutateAsync({
                  projectId: project.id,
                  req: mapDatasetFormToRequest(data),
                });
              }}
              onSuccess={close}
            />
          )}
        </CreateDialog>
      </header>
    </div>
  );
}
