import { ChevronRight } from "lucide-react";
import { Badge } from "../ui/badge";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "../ui/item";

export default function ProjectItem({
  item,
  isActive,
  onClick,
}: {
  item: any;
  isActive: boolean;
  onClick: () => void;
}) {
  return (
    <Item
      variant="outline"
      onClick={onClick}
      className={`w-full text-left px-3 py-2.5 rounded-lg border transition-all ${
        isActive
          ? 'bg-violet-50 border-violet-300 border-l-violet-500 border-l-4 '
          : 'bg-white border-zinc-100 hover:border-violet-200 hover:bg-violet-50/50'
      }`}
      // className={cn(
      //   "cursor-pointer hover:bg-muted rounded-sm",
      //   isActive && "bg-[#f5f5fd] border-[#7f77dd] border-l-4",
      // )}
    >
      {/* <span
          className={`w-1.5 h-1.5 rounded-full shrink-0 ${
            isActive ? 'bg-violet-500' : 'bg-zinc-300'
          }`}
        /> */}
      <ItemContent>
        <ItemTitle className="font-medium">
          {item.name}
        </ItemTitle>
        <ItemDescription className="text-xs">
          {item.description}
        </ItemDescription>
        <div className="flex gap-2">
          <Badge variant="outline">데이터셋 {item.dataset}</Badge>
          <Badge variant="outline">시나리오 {item.scenario}</Badge>
          <Badge variant="outline">프롬프트 {item.prompt}</Badge>
        </div>
      </ItemContent>
      <ItemActions>
        <ChevronRight className={`w-3 h-3 shrink-0 ${isActive ? 'text-violet-400' : 'text-zinc-300'}`} />
      </ItemActions>
    </Item>
  );
}
