import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import CreateDialog from "@/components/common/dialogs/CreateDialog";
import CreatePromptForm from "./CreatePromptForm";
import { useCreatePromptMutation } from "../hooks/usePromptsMutation";
import type { Project } from "@/features/projects/models/model";
import type { Dataset } from "@/features/datasets/models";
import { mapPromptFormToRequest } from "../api/prompt.mapper";

export default function PromptHeader({
  project,
  dataset,
}: {
  project: Project;
  dataset: Dataset;
}) {
  const { mutateAsync } = useCreatePromptMutation();

  return (
    <div className="p-3">
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
      <div className="flex justify-between pt-3">
        <h2 className="mb-1 text-xl font-bold text-[#16192b]">프롬프트</h2>
        <CreateDialog title="프롬프트" formId="prompt-form">
          {(close) => (
            <CreatePromptForm
              formId="prompt-form"
              onSubmit={async (data) => {
                await mutateAsync({ projectId: project.id, req: mapPromptFormToRequest(data) })
              }}
              onSuccess={close}
            />
          )}
        </CreateDialog>
      </div>
    </div>
  );
}
