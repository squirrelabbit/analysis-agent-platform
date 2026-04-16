import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Clock,
  Download,
  File,
  FileJson,
  FileSpreadsheet,
  FileText,
  HardDrive,
  Trash2,
} from "lucide-react";

function fileIcon(name: string) {
  const ext = name.split(".").pop()?.toLowerCase();
  if (["csv", "xlsx", "xls"].includes(ext ?? ""))
    return <FileSpreadsheet className="w-3.5 h-3.5" />;
  if (["json", "jsonl"].includes(ext ?? ""))
    return <FileJson className="w-3.5 h-3.5" />;
  if (["txt", "md"].includes(ext ?? ""))
    return <FileText className="w-3.5 h-3.5" />;
  return <File className="w-3.5 h-3.5" />;
}

/** 파일 한 행 */
export function FileRow({
  file,
  isLatest,
}: {
  file: any;
  isLatest: boolean;
}) {
  // const cfg = STATUS_CONFIG["processing"];
  return (
    <Item
      variant="muted"
      className="group border border-gray-200 rounded-lg  hover:bg-gray-100"
    >
      <ItemMedia className="w-7 h-7 rounded-md bg-zinc-100 shrink-0 group-hover:bg-gray-200">
        {fileIcon(file.stored_filename)}
      </ItemMedia>
      <ItemContent>
        <ItemTitle>{file.stored_filename}</ItemTitle>
        <ItemDescription>
          <Button variant="ghost" size="xs" disabled className="p-0 text-[10px]">
            <HardDrive />
              {file.byte_size}
          </Button>
          <Button variant="ghost" size="xs" disabled className="text-[10px]">
            <Clock />
              {file.uploaded_at.slice(0, 10)} 
          </Button>
        </ItemDescription>
      </ItemContent>
      <ItemActions>
        {isLatest && <Badge variant="outline" className="text-[11px] bg-green-50 text-green-700">활성</Badge>}
        {/* 업로드 버튼 */}
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="p-1 rounded-lg hover:bg-violet-100 hover:text-violet-600 text-zinc-400 transition-colors"
              >
                <Download className="w-3.5 h-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="top">
              <p className="text-xs">파일 다운로드</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </ItemActions>
      <ItemActions>
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                size="sm"
                variant="ghost"
                className="p-1 rounded-md hover:bg-red-50 hover:text-red-500 text-zinc-400 transition-all"
              >
                <Trash2 className="w-3.5 h-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="top">
              <p className="text-xs">버전 삭제</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </ItemActions>
    </Item>
  );
}
