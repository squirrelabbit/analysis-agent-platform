import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemHeader,
  ItemTitle,
} from "@/components/ui/item";
import type { Project } from "../types/project";
import { useNavigate } from "react-router-dom";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";
import { useRemoveProjectMutation } from "../hooks/useProjectsMutation";

export default function ProjectItem(props: Project) {
  const navigate = useNavigate();
  const { mutate } = useRemoveProjectMutation();
  const { id, name, description, datasetCount, scenarioCount, promptCount } =
    props;

  const badges = [
    { label: "데이터셋", count: datasetCount },
    { label: "시나리오", count: scenarioCount },
    { label: "프롬프트", count: promptCount },
  ];

  return (
    <Item
      key={id}
      variant="outline"
      className="bg-white cursor-pointer hover:-translate-y-0.5 hover:border-indigo-500"
      onClick={() => navigate(`/projects/${id}`)} // Item 클릭 → 페이지 이동
    >
      <ItemHeader>
        <ItemContent>
          <ItemTitle className="font-bold">{name}</ItemTitle>
          <ItemDescription className=" text-xs text-zinc-400">
            {description}
          </ItemDescription>
        </ItemContent>
        <ItemActions onClick={(e) => e.stopPropagation()}>
          <DeleteDialog onDelete={() => mutate(id)} title="프로젝트">
            <div>프로젝트 이름 : {name}</div>
          </DeleteDialog>
        </ItemActions>
      </ItemHeader>
      <ItemContent>
        <div className="flex gap-1.5 flex-wrap">
          {badges.map(({ label, count }, idx) => (
            <Badge
              key={idx}
              variant="outline"
              className={cn(
                "text-[10px]",
                count !== 0
                  ? "bg-indigo-50 text-indigo-500"
                  : "text-zinc-400 bg-zinc-100",
              )}
            >
              {label} {count}
            </Badge>
          ))}
        </div>
      </ItemContent>
    </Item>
  );
}
