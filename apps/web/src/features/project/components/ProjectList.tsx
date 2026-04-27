import type { Project } from "../types/project";
import { FolderOpen, LayoutGrid, Rows3 } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import ProjectItem from "./ProjectItem";

interface ProjectListProps {
  total: number;
  projects: Project[];
}

export default function ProjectList({ total, projects }: ProjectListProps) {
  if (projects.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center gap-1.5 py-16 text-center px-4">
        <FolderOpen className="text-zinc-300" />
        <p className="text-sm font-medium text-zinc-500">
          {total === 0
            ? "등록된 프로젝트가 없습니다"
            : "프로젝트 검색 결과가 없습니다"}
        </p>
        <p className="text-xs text-zinc-400">
          {total === 0
            ? "상단 버튼으로 프로젝트를 먼저 등록해주세요"
            : "다른 키워드로 검색해보세요"}
        </p>
      </div>
    );
  }

  return (
    <Tabs defaultValue="row">
      <TabsList>
        <TabsTrigger value="row">
          <Rows3 />
        </TabsTrigger>
        <TabsTrigger value="grid">
          <LayoutGrid />
        </TabsTrigger>
      </TabsList>
      <TabsContent value="row">
        <div className="flex flex-col gap-2">
          {projects.map((project) => (
            <ProjectItem key={project.id} {...project} />
          ))}
        </div>
      </TabsContent>
      <TabsContent value="grid">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
          {projects.map((project) => (
            <ProjectItem key={project.id} {...project} />
          ))}
        </div>
      </TabsContent>
    </Tabs>
  );
}
