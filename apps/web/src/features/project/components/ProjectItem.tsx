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
import { cn } from "@/lib/utils";
import { ChevronRight } from "lucide-react";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";

interface Props {
  project: Project;
  isActive: boolean;
  onDelete: (id: string) => void;
}

export default function ProjectItem({ project, isActive, onDelete }: Props) {
  const { id, name, description, datasetCount, scenarioCount, promptCount } =
    project;
  const navigate = useNavigate();

  return (
    <Item
      key={id}
      variant="outline"
      onClick={() => navigate(`/projects/${id}?tab=dataset`)}
      className={cn(
        "flex items-center cursor-pointer gap-2.5 px-3 py-2.5 rounded-lg text-left transition-all",
        isActive
          ? "bg-violet-50 border-violet-200 text-violet-800"
          : "hover:bg-zinc-50 text-zinc-700",
      )}
    >
      <ItemHeader>
        <ItemMedia className="flex size-8 items-center justify-center rounded-lg border text-base font-semibold leading-none">
          {name[0].toUpperCase()}
        </ItemMedia>
        <ItemContent>
          <ItemTitle className="font-bold">{name}</ItemTitle>
          <ItemDescription className="text-[11px] text-zinc-400">
            {description}
          </ItemDescription>
        </ItemContent>
        <ItemActions>
          <DeleteDialog onDelete={() => onDelete(id)} title="프로젝트">
            <div>프로젝트 이름 : {name}</div>
          </DeleteDialog>
          <ChevronRight
            className={`w-3.5 h-3.5 shrink-0 ${isActive ? "text-violet-400" : "text-zinc-300"}`}
          />
        </ItemActions>
      </ItemHeader>
      <ItemContent>
        <div className="flex gap-1.5 flex-wrap">
          {[
            { label: "데이터셋", count: datasetCount },
            { label: "시나리오", count: scenarioCount },
            { label: "프롬프트", count: promptCount },
          ].map(({ label, count }) => (
            <span
              key={label}
              className={cn(
                "text-[9px] px-1.5 py-0.5 rounded-full border font-medium",
                count !== 0
                  ? isActive
                    ? "bg-violet-100 text-violet-700 border-violet-200"
                    : "bg-zinc-100 text-zinc-500 border-zinc-200"
                  : "bg-red-100 text-red-700 border-red-200",
              )}
            >
              {label} {count}
            </span>
          ))}
        </div>
      </ItemContent>
    </Item>
  );
}
