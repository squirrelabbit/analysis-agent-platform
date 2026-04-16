import DetailPanel from "@/components/projects/DetailPanel";
import ProjectHead from "@/components/projects/ProjectHead";
import ProjectList from "@/components/projects/ProjectList";
import { Separator } from "@/components/ui/separator";
import { useProjects } from "@/hooks/useProject";
import { memo } from "react";

// searchQuery 바뀔 때 DetailPanel 리렌더링 차단
const MemoDetailPanel = memo(DetailPanel);

export default function ProjectPage() {
  const {
    projects,
    filtered,
    selectedProject,
    selectedId,
    selectProject,
    searchQuery,
    setSearchQuery,
  } = useProjects();

  return (
    <div className="flex gap-3">
      <div className="flex-1">
        <ProjectHead
          total={projects?.length || 0}
          filteredCount={filtered?.length || 0}
          searchQuery={searchQuery}
          onSearchChange={setSearchQuery}
        />
        <ProjectList
          filtered={filtered}
          isExist={projects?.length == 0}
          selectedId={selectedId}
          onClick={(p) => selectProject(p)}
        />
      </div>
      {selectedProject && <Separator orientation="vertical" />}
      {selectedProject && (
        <div className="flex-2">
          <MemoDetailPanel {...selectedProject} />
        </div>
      )}
    </div>
  );
}
