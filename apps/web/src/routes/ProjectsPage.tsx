import { Skeleton } from "@/components/ui/skeleton";
import ProjectDetailPanel from "@/features/project/components/ProjectDetailPanel";
import ProjectHeader from "@/features/project/components/ProjectHeader";
import ProjectList from "@/features/project/components/ProjectList";
import { useProjects } from "@/features/project/hooks/useProjects";
import { FolderOpen } from "lucide-react";
import { memo, useMemo, useState } from "react";
import { useParams } from "react-router-dom";

// search 바뀔 때 DetailPanel 리렌더링 차단
const MemoDetailPanel = memo(ProjectDetailPanel);

export default function ProjectsPage() {
  const { id } = useParams();

  const { data: projects = [], create, remove, isLoading } = useProjects();
  const [search, setSearch] = useState("");

  const filteredProjects = useMemo(() => {
    return projects.filter((p) =>
      p.name.toLowerCase().includes(search.toLowerCase()),
    );
  }, [projects, search]);

  const selectedProject = projects.find((project) => project.id === id)
  return (
    <div className="flex h-full">
      {/* ── 좌: 프로젝트 목록 ──────────────────────────────── */}
      <aside className="flex flex-col w-xs border-r p-4 gap-4 border-zinc-200 bg-white">
        {/* 헤더 */}
        <ProjectHeader
          search={search}
          onChangeSearch={(v) => setSearch(v)}
          totalCount={projects.length}
          filteredCount={filteredProjects.length}
          onCreate={(name, description) => create.mutate({ name, description })}
        />
        {/* 목록 */}
        {isLoading ? (
          <div className="flex flex-col gap-2">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-16 w-full" />
            ))}
          </div>
        ) : (
          <ProjectList
            total={projects.length}
            projects={filteredProjects}
            selectedId={id ?? null}
            onRemove={(id) => remove.mutate(id)}
          />
        )}
      </aside>

      {/* ── 우: 프로젝트 상세 ──────────────────────────────── */}
      <main className="flex-1 min-w-0 bg-zinc-100 border-l border-zinc-100 overflow-hidden">
        {selectedProject ? <MemoDetailPanel {...selectedProject}/> 
        :
          <div className="flex flex-col items-center justify-center h-full gap-4 text-center text-zinc-400">
            <FolderOpen className="w-12 h-12 opacity-30" />
            <div>
              <p className="text-sm font-medium text-zinc-500">
                프로젝트를 선택하세요
              </p>
              <p className="text-xs mt-1">
                왼쪽 목록에서 프로젝트를 클릭하면 상세 내용이 표시됩니다
              </p>
            </div>
          </div>
        }
      </main>
    </div>
  );
}
