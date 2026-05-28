import { useDataset } from "../hooks/dataset.query";

import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import HeadBreadcrumb from "../components/HeadBreadcrumb";
import { VersionList } from "@/features/versions/components/VersionList";
import CreateDialog from "@/components/common/dialogs/CreateDialog";
import CreateVersionForm from "@/features/versions/components/forms/CreateVersionForm";
import { useCreateVersion } from "@/features/versions/hooks/version.mutation";

export default function DatasetDetail() {
  const { projectId } = useProjectParams();
  const { data: project } = useProjectDetail(projectId);
  const { data: dataset } = useDataset();
  const { mutateAsync} = useCreateVersion()

  if (!dataset)return null
  return (
    <div className="p-8">
      {/* 데이터셋 정보 */}
      <HeadBreadcrumb project={project!} dataset={dataset} />
      <div className="flex items-center justify-between mt-4 mb-6">
        <div>
          <h2 className="text-xl font-bold">{dataset?.name}</h2>
          <p className="text-sm text-[#9399b0]">{dataset?.description}</p>
        </div>
        <CreateDialog title="데이터" formId="version-form">
          {(close) => (
            <CreateVersionForm
            formId='version-form'
            type={dataset.dataType}
            onSubmit={async (data) => {mutateAsync(data)}}
            onSuccess={close}
            />
          )}
        </CreateDialog>
      </div>
      <VersionList  />
    </div>
  );
}
