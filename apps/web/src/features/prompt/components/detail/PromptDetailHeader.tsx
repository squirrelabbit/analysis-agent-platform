import { OperationBadge, StatusBadge } from "../OperationBadge";
import { VersionPicker } from "./VersionPicker";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Trash2, Calendar, RefreshCw } from "lucide-react";
import type { Prompt, PromptGroup } from "../../types/prompt";
import { fmtDate } from "../../utils/prompt";

export type ViewMode = "read" | "edit" | "diff";

interface Props {
  group: PromptGroup;
  activePrompt: Prompt;
  viewMode: ViewMode;
  onVersionChange: (version: string) => void;
  onViewModeChange: (mode: ViewMode) => void;
  onAddVersion: () => void;
  onDelete: () => void;
}

export function PromptDetailHeader({
  group,
  activePrompt,
  viewMode,
  onVersionChange,
  onViewModeChange,
  onAddVersion,
  onDelete,
}: Props) {
  return (
    <div className="px-6 py-4 border-b border-border shrink-0">
      {/* 행 1: operation + 제목 + 액션 */}
      <div className="flex items-start gap-3 mb-3">
        <OperationBadge
          operation={group.operation}
          className="mt-0.5 shrink-0"
        />
        <h2 className="text-[18px] font-bold tracking-tight flex-1 leading-snug">
          {group.title}
        </h2>
        <div className="flex items-center gap-2 shrink-0">
          {/* 보기 모드 탭 */}
          <Tabs
            value={viewMode}
            onValueChange={(v) => onViewModeChange(v as ViewMode)}
          >
            <TabsList className="h-8">
              <TabsTrigger value="read"    className="text-[11px] px-3">보기</TabsTrigger>
              <TabsTrigger value="edit"    className="text-[11px] px-3">편집</TabsTrigger>
              <TabsTrigger value="diff"    className="text-[11px] px-3">버전 비교</TabsTrigger>
            </TabsList>
          </Tabs>

          <Button
            size="sm"
            variant="outline"
            className="h-8 text-[11px] gap-1.5 text-destructive
                       border-destructive/30 hover:bg-destructive/5"
            onClick={onDelete}
          >
            <Trash2 className="w-3 h-3" />
            삭제
          </Button>
        </div>
      </div>

      {/* 행 2: 버전 선택기 */}
      <div className="mb-3">
        <VersionPicker
          versions={group.versions}
          activeVersion={activePrompt.version}
          onChange={onVersionChange}
          onAddVersion={onAddVersion}
        />
      </div>

      {/* 행 3: 메타 */}
      <div className="flex items-center gap-4 text-[11px] text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <Calendar className="w-3.5 h-3.5" />
          등록 {fmtDate(activePrompt.createdAt)}
        </span>
        <span className="flex items-center gap-1.5">
          <RefreshCw className="w-3.5 h-3.5" />
          수정 {fmtDate(activePrompt.updatedAt)}
        </span>
        <StatusBadge status={activePrompt.status} />
      </div>
    </div>
  );
}