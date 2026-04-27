import CreateDialog from "@/components/common/dialogs/CreateDialog";
import { useCreateProjectMutation } from "../hooks/useProjectsMutation";
import CreateProjectForm from "./CreateProjectForm";

export default function ProjectHeader({}) {
  const create = useCreateProjectMutation();

  return (
    <header className="flex justify-between">
      <div>
        <h2 className="mb-1 text-xl font-bold text-[#16192b]">프로젝트</h2>
        <p className="text-sm text-[#9399b0]">
          분석 단위 프로젝트를 생성하고 선택합니다.
        </p>
      </div>
      <div>
        <CreateDialog title="프로젝트" formId="project-form">
          {(close) => (
            <CreateProjectForm
              formId="project-form"
              onSubmit={async (data) => {
                await create.mutateAsync(data);
              }}
              onSuccess={close}
            />
          )}
        </CreateDialog>
      </div>
    </header>
  );
}
