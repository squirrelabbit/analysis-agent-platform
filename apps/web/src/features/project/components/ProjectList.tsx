import { ScrollArea } from "@/components/ui/scroll-area";
import type { Project } from "../types/project";
import { FolderOpen } from "lucide-react";
import ProjectItem from "./ProjectItem";

interface Props {
  total: number;
  projects: Project[];
  selectedId: string | null;
  onRemove: (id: string) => void;
}

export default function ProjectList({
  total,
  projects,
  selectedId,
  onRemove,
}: Props) {
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
    <ScrollArea className="h-full">
      <div className="flex flex-col gap-2">
        {projects.map((p, idx) => {
          const isActive = selectedId === p.id;
          return (
            <ProjectItem
              key={idx}
              project={p}
              isActive={isActive}
              onDelete={onRemove}
            />
          );
        })}
      </div>
    </ScrollArea>
  );
}
