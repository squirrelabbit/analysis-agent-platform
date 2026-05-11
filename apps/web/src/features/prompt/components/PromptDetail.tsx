import { Item, ItemContent, ItemHeader, ItemTitle } from "@/components/ui/item";
import type { Prompt } from "../types/prompt";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";

export default function PromptDetail({ prompt }: { prompt: Prompt }) {
  return (
    <Item>
      <ItemHeader>
        <ItemTitle>
          <div className="flex-1 flex items-center gap-4 py-3 flex-wrap">
            <Badge>{prompt.operation}</Badge>
            <Badge>{prompt.version}</Badge>
            <p>업데이트 {prompt.updatedAt}</p>
          </div>
        </ItemTitle>
      </ItemHeader>
      <ItemContent>
        <Textarea className="bg-white" value={prompt.content} />
      </ItemContent>
    </Item>
  );
}
