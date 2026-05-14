import { Outlet } from "react-router-dom";
import Sidebar from "./Sidebar";
import { useProjectId } from "@/hooks/useProjectId";
import { useProjectDetail } from "@/features/project/hooks/project.query";

export default function ProjectLayout() {
  const { projectId } = useProjectId();
  const { data: project } = useProjectDetail(projectId);

  if (!project) return null;
  return (
    <div className="flex h-[calc(100vh-56px)]">
      <Sidebar project={project} />

      <div className="flex-1 overflow-auto">
        <Outlet context={{ project }} />
      </div>
    </div>
  );
}
