import {
  Item,
  ItemContent,
  ItemHeader,
} from "@/components/ui/item";
import type { Prompt } from "../types/prompt";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Calendar } from "lucide-react";

export default function PromptItem({
  prompt,
  isSelected,
  onSelect,
}: {
  prompt: Prompt;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const { title, status, operation, version, createdAt, updatedAt } = prompt;
  return (
    <Item
      onClick={onSelect}
      className={cn(
        "hover:bg-blue-50",
        isSelected && "bg-blue-50 hover:bg-blue-50",
      )}
    >
      <ItemHeader>
        {title}
        <Badge variant="outline" className={cn("h-4 px-1.5 text-[10px]")}>
          {version}
        </Badge>
        <Badge className={cn("h-4 px-1.5 text-[10px]")}>{operation}</Badge>

      </ItemHeader>
      <ItemContent>
        {/* 메타 정보 */}
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span className="flex items-center gap-1">
            <Calendar className="w-2.5 h-2.5" />
            등록일 {createdAt.slice(0, 10)}
          </span>{" "}
          ·
          <span className="flex items-center gap-1">
            <Calendar className="w-2.5 h-2.5" />
            수정일 {updatedAt.slice(0, 10)}
          </span>{" "}
          ·<span className="flex items-center gap-1">{status}</span>{" "}
        </div>
      </ItemContent>
    </Item>
  );
}
