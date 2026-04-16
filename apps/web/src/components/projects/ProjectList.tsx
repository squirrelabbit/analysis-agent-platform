import { ScrollArea } from "../ui/scroll-area";
import ProjectItem from "./ProjectItem";
import type { Project } from "@/types";
import { EmptyForm } from "../common/EmptyForm";
import { FolderOpen } from "lucide-react";

export default function ProjectList({
  filtered,
  isExist,
  selectedId,
  onClick,
}: {
  filtered: Project[];
  isExist: boolean;
  selectedId: string | null;
  onClick: (project: Project) => void;
}) {
  return filtered.length == 0 ? (
    <EmptyForm
      title={isExist ? "등록된 프로젝트가 없습니다" : "검색 결과가 없습니다"}
      description={
        isExist ? "프로젝트를 먼저 등록해주세요" : "다른 키워드로 검색해보세요"
      }
      icon={<FolderOpen className="text-zinc-400" />}
    />
  ) : (
    <ScrollArea className="h-140">
      <div className="flex flex-col gap-2">
        {filtered.map((item, idx) => (
          <ProjectItem
            key={idx}
            project={item}
            isSelected={selectedId === item.project_id}
            onClick={() => onClick(item)}
          />
        ))}
      </div>
    </ScrollArea>
  );
}
