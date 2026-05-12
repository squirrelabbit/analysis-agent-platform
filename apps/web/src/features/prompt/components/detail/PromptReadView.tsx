import { Card } from "@/components/ui/card";

interface Props { content: string; }

export function PromptReadView({ content }: Props) {
  return (
    <Card className="m-4">

    <div className="flex-1 overflow-y-auto px-6 py-5">
      <pre className="font-mono text-[12px] leading-[1.8]
                      text-foreground whitespace-pre-wrap wrap-break-word">
        {content.trim()}
      </pre>
    </div>
    </Card>
  );
}