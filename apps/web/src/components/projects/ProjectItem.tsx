import { ChevronRight } from "lucide-react";
import { Badge } from "../ui/badge";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "../ui/item";

function WarningBadge({ title, count }: { title: string; count: number }) {
  const isWarning = count === 0;
  return (
    <Badge className="text-[11px]"  variant={isWarning ? "destructive" : "outline"}>
      {title} {count}
    </Badge>
  );
}

export default function ProjectItem({
  project,
  isSelected,
  onClick,
}: {
  project: any;
  isSelected: boolean;
  onClick: () => void;
}) {
  return (
    <Item
      variant="outline"
      onClick={onClick}
      className={`w-full text-left px-3 py-2.5 rounded-lg border transition-all ${
        isSelected
          ? "bg-violet-50 border-violet-300 border-l-violet-500 border-l-4 "
          : "bg-white border-zinc-100 hover:border-violet-200 hover:bg-violet-50/50"
      }`}
    >
      <ItemContent>
        <ItemTitle>{project.name}</ItemTitle>
        <ItemDescription>{project.description}</ItemDescription>
        <div className="flex gap-1.5 pt-1">
          <WarningBadge title="데이터셋" count={project.dataset_version_count} />
          <WarningBadge title="시나리오" count={project.scenario_count} />
          <WarningBadge title="프롬프트" count={project.prompt_count} />
        </div>
      </ItemContent>
      <ItemActions >
        <ChevronRight
          className={`w-3 h-3 shrink-0 ${isSelected ? "text-violet-400" : "text-zinc-300"}`}
        />
      </ItemActions>
    </Item>
  );
}
