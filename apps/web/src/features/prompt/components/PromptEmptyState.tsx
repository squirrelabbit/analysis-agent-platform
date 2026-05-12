import { Code2 } from "lucide-react";

export function PromptEmptyState() {
  return (
    <div className="flex flex-col items-center justify-center
                    flex-1 gap-3 text-muted-foreground">
      <Code2 className="w-10 h-10 opacity-20" />
      <p className="text-[12px]">프롬프트를 선택하세요</p>
    </div>
  );
}