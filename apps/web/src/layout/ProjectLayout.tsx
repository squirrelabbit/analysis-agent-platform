import { Outlet } from "react-router-dom";
import Sidebar from "./Sidebar";
import { useProjectId } from "@/hooks/useProjectId";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { ChatNavProvider } from "@/features/chats/context/ChatNavProvider";

export default function ProjectLayout() {
  const { projectId } = useProjectId();
  const { data: project } = useProjectDetail(projectId);

  if (!project) return null;
  return (
    <div className="flex h-[calc(100vh-56px)]">
      {/* 대화 이력을 사이드바와 채팅 본문이 공유하도록 둘 다 감싼다. */}
      <ChatNavProvider>
        <Sidebar project={project} />

        <div className="flex-1 overflow-auto">
          <Outlet context={{ project }} />
        </div>
      </ChatNavProvider>
    </div>
  );
}
