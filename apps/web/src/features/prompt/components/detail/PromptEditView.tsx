import { useState } from "react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Info, Save } from "lucide-react";

interface Props {
  initialContent: string;
  onSave: (content: string) => void;
  onCancel: () => void;
}

export function PromptEditView({ initialContent, onSave, onCancel }: Props) {
  const [content, setContent] = useState(initialContent);

  return (
    <div className="flex flex-col flex-1 overflow-hidden">
      {/* 안내 */}
      <div className="px-3 pt-4 shrink-0">
        <Alert className="p-3 border-primary/20 bg-primary/5">
          <Info className="w-3.5 h-3.5 text-primary" />
          <AlertDescription className="text-[11px] text-primary">
            저장하면 <strong>새 버전</strong>이 자동 생성됩니다.
            기존 버전은 보존됩니다.
          </AlertDescription>
        </Alert>
      </div>

      {/* 에디터 */}
      <div className="flex-1 overflow-hidden p-3">
        <Textarea
          value={content}
          onChange={(e) => setContent(e.target.value)}
          className="h-full resize-none font-mono text-[12px]
                      border-border bg-white"
          spellCheck={false}
        />
      </div>

      {/* 푸터 */}
      <div className="px-6 py-3 border-t border-border bg-muted/30
                      flex items-center justify-between shrink-0">
        <span className="text-[11px] text-muted-foreground font-mono">
          {content.length.toLocaleString("ko-KR")}자
        </span>
        <div className="flex gap-2">
          <Button size="sm" variant="outline"
                  className="h-7 text-[11px]"
                  onClick={onCancel}>
            취소
          </Button>
          <Button size="sm"
                  className="h-7 text-[11px] gap-1.5"
                  onClick={() => onSave(content)}>
            <Save className="w-3 h-3" />
            새 버전으로 저장
          </Button>
        </div>
      </div>
    </div>
  );
}