import { useMemo, useState } from "react";
import type { Project } from "../types/project";
import ProjectToolbar from "./ProjectToolbar";
import { ProjectItem } from "./ProjectItem";
import { ItemGroup } from "@/components/ui/item";
import { useCreateProjectMutation } from "../hooks/project.mutation";
import ProjectHeader from "./ProjectHeader";
import { FolderOpen } from "lucide-react";
import PaginationControls from "./PaginationControls";
import { cn } from "@/lib/utils";
import CreateDialog from "@/components/common/dialogs/CreateDialog";
import CreateProjectForm from "../forms/CreateProjectForm";
import ProjectItemSkeleton from "./ProjectItemSkeleton";

interface ProjectContainerProps {
  projects: Project[];
  isLoading: boolean;
}

export default function ProjectContainer({
  projects,
  isLoading,
}: ProjectContainerProps) {
  const { mutateAsync } = useCreateProjectMutation();

  // 상태 관리
  const [search, setSearch] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(10);
  const [viewMode, setViewMode] = useState<"grid" | "list">("grid");

  // 검색 필터링
  const filteredProjects = useMemo(() => {
    if (!search.trim()) return projects;
    const query = search.toLowerCase();
    return projects.filter(
      ({ name, description }) =>
        name.toLowerCase().includes(query) ||
        description.toLowerCase().includes(search),
    );
  }, [projects, search]);

  // 페이징 계산
  const totalPages = Math.ceil(filteredProjects.length / itemsPerPage);
  const paginatedProjects = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return filteredProjects.slice(startIndex, endIndex);
  }, [filteredProjects, currentPage, itemsPerPage]);

  // 페이지 변경 시 유효성 검사
  const handlePageChange = (page: number) => {
    const validPage = Math.max(1, Math.min(page, totalPages || 1));
    setCurrentPage(validPage);
  };

  // 항목 개수 변경
  const handleItemsPerPageChange = (count: number) => {
    setItemsPerPage(count);
    setCurrentPage(1); // 첫 페이지로 리셋
  };

  return (
    <div>
      <ProjectHeader />
      <ProjectToolbar
        search={search}
        onSearchChange={(v) => setSearch(v)}
        view={viewMode}
        onViewChange={(v) => setViewMode(v)}
        pageSize={itemsPerPage}
        onPageSizeChange={handleItemsPerPageChange}
        totalCount={filteredProjects.length}
      />
      {isLoading ? (
        <ProjectItemSkeleton view={viewMode} />
      ) : (
        <>
          <main>
            {paginatedProjects.length === 0 ? (
              // 빈 상태
              <div className="flex flex-col items-center justify-center py-16">
                <FolderOpen className="text-zinc-400 mb-4" />
                <div className="text-center">
                  <h3 className="text-xl font-semibold text-slate-900 mb-2">
                    {search ? "검색 결과가 없습니다" : "프로젝트가 없습니다"}
                  </h3>
                  <p className="text-slate-600 mb-6">
                    {search
                      ? `"${search}"에 해당하는 프로젝트를 찾을 수 없습니다.`
                      : "새로운 프로젝트를 생성하여 시작하세요."}
                  </p>
                  {!search && (
                    <CreateDialog title="프로젝트" formId="project-form">
                      {(close) => (
                        <CreateProjectForm
                          formId="project-form"
                          onSubmit={async (data) => {
                            await mutateAsync(data);
                          }}
                          onSuccess={close}
                        />
                      )}
                    </CreateDialog>
                  )}
                </div>
              </div>
            ) : (
              <ItemGroup
                key={`${viewMode}-${search}`}
                className={cn(
                  "animate-in fade-in duration-300",
                  `${viewMode === "grid" && "grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 auto-rows-max"}`,
                )}
              >
                {paginatedProjects.map((project, index) => (
                  <div
                    key={project.id}
                    className="animate-in fade-in slide-in-from-bottom-4 duration-500"
                    style={{
                      animationDelay: `${index * 50}ms`,
                      animationFillMode: "both",
                    }}
                  >
                    <ProjectItem view={viewMode} project={project} />
                  </div>
                ))}
              </ItemGroup>
            )}
          </main>
          {filteredProjects.length > 0 && (
            <PaginationControls
              currentPage={currentPage}
              onPageChange={handlePageChange}
              totalPages={totalPages}
            />
          )}
        </>
      )}
    </div>
  );
}
