import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemHeader,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import type { Project } from "../types/project";
import { useNavigate } from "react-router-dom";
import { useDeleteProjectMutation } from "../hooks/project.mutation";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Calendar, ChevronRight, X } from "lucide-react";
import { fmtDate } from "@/utils/format";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";

interface ProjectItemViewProps {
  project: Project;
  counts: { label: string; count: number }[];
  onDelete: () => void;
}
export function ProjectItem({
  project,
  view,
}: {
  project: Project;
  view: "grid" | "list";
}) {
  const navigate = useNavigate();
  const { mutateAsync } = useDeleteProjectMutation();

  const BADGES = [
    { label: "데이터셋", count: project.datasetCount },
    { label: "시나리오", count: project.scenarioCount },
    { label: "프롬프트", count: project.promptCount },
  ];

  const handleDelete = () => {
    mutateAsync(project.id);
  };

  return (
    <Item
      variant="outline"
      className="group bg-white hover:cursor-pointer hover:-translate-y-0.5 hover:border-violet-200 hover:bg-violet-50 hover:shadow-2xl"
      onClick={() => navigate(`/projects/${project.id}`)}
    >
      {view === "grid" ? (
        <ProjectGridItem
          project={project}
          counts={BADGES}
          onDelete={handleDelete}
        />
      ) : (
        <ProjectListItem
          project={project}
          counts={BADGES}
          onDelete={handleDelete}
        />
      )}
    </Item>
  );
}

function ProjectGridItem({ project, counts, onDelete }: ProjectItemViewProps) {
  const { name, description, createdAt } = project;
  // 프로젝트 이름의 첫 글자로 아바타 생성
  const avatarLetter = project.name.charAt(0).toUpperCase();
  
  return (
    <>
      <ItemHeader>
        <ItemMedia
          variant={"default"}
          className="h-10 w-10 rounded-md bg-violet-100 text-violet-700"
        >
          {avatarLetter}
        </ItemMedia>
        <ItemContent className="pl-2">
          <ItemHeader>
            <div>
              <ItemTitle className="font-bold font-mono text-[16px]">
                {name}
              </ItemTitle>
              <ItemDescription className="text-xs">
                {description}
              </ItemDescription>
            </div>

            <ItemActions
              className=" text-zinc-400 opacity-0 transition-opacity group-hover:opacity-100"
              onClick={(e) => e.stopPropagation()}
            >
              <DeleteDialog title="프로젝트" onDelete={onDelete}>
                <div>프로젝트명: {name}</div>
              </DeleteDialog>
              <ChevronRight className="w-5 h-5 group-hover:text-violet-600" />
            </ItemActions>
          </ItemHeader>
          <span className="flex items-center gap-1 text-[11px] text-zinc-500 pt-1">
            <Calendar className="w-3 h-3" />
            {fmtDate(createdAt)}
          </span>
        </ItemContent>
      </ItemHeader>
      <Separator className="my-1.5" />
      <div className="flex gap-2 ">
        {counts.map(({ label, count }) => (
          <Badge
            key={label}
            variant="secondary"
            className="text-xs text-zinc-500"
          >
            <span
              className={`w-1.5 h-1.5 rounded-full shrink-0 mr-1 ${!count ? "bg-zinc-300" : "bg-emerald-500"}`}
            />
            {label} {count}
          </Badge>
        ))}
      </div>
    </>
  );
}

function ProjectListItem({ project, counts, onDelete }: ProjectItemViewProps) {
  const { name, description } = project;
  return (
    <>
      <ItemHeader>
        <div>
          <ItemTitle className="font-bold font-mono text-[16px]">{name}</ItemTitle>
          <ItemDescription className="text-xs">{description}</ItemDescription>
        </div>
        <ItemActions onClick={(e) => e.stopPropagation()}>
          <div className="flex gap-2 ">
            {counts.map(({ label, count }) => (
              <Badge
                key={label}
                variant="secondary"
                className="text-xs text-zinc-500"
              >
                <span
                  className={`w-1.5 h-1.5 rounded-full shrink-0 mr-1 ${!count ? "bg-zinc-300" : "bg-emerald-500"}`}
                />
                {label} {count}
              </Badge>
            ))}
          </div>
          <DeleteDialog title="프로젝트 삭제" onDelete={onDelete} Icon={<X />}>
            <div>프로젝트명: {name}</div>
          </DeleteDialog>
        </ItemActions>
      </ItemHeader>
    </>
  );
}
