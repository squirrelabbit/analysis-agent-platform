import type { Project } from "@/features/projects/models/model";
import HeadBreadcrumb from "./HeadBreadcrumb";
import CreateDatasetDialog from "./CreateDatasetDialog";

export default function DatasetHeader(project: Project) {
  return (
    <div>
      <HeadBreadcrumb project={project} />
      <header className="flex justify-between py-4">
        <div>
          <h2 className="text-xl font-bold">데이터셋</h2>
          <p className="text-sm text-[#9399b0]">
            이 프로젝트에서 사용할 데이터셋을 등록·관리합니다. 데이터셋별로
            프롬프트와 업로드 데이터가 연결됩니다.
          </p>
        </div>
        <CreateDatasetDialog />
      </header>
    </div>
  );
}
