import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Database, Info, Trash2, Upload } from "lucide-react";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import { useState } from "react";
import { DatasetDialog } from "./DatasetDialog";
import type { Dataset } from "@/types";
import type { DatasetVersionResponse } from "@/types/dto/dataset.dto";

export function DatasetItem({
  dataset,
  fetchVersions,
  versions,
}: {
  dataset: Dataset;
  fetchVersions: (id: string) => void;
  versions: DatasetVersionResponse[];
}) {
  const [open, setOpen] = useState(false);
  const handleClick = async (datasetId: string) => {
    await fetchVersions(datasetId)
    setOpen(true);
  };

  return (
    <Item className="border border-zinc-100 rounded-lg  hover:bg-violet-50 hover:border-violet-200 transition-colors ">
      <ItemMedia className="w-8 h-8 rounded-lg bg-violet-100 ">
        <Database className="w-4 h-4 text-violet-600" />
      </ItemMedia>

      <ItemContent>
        <ItemTitle>{dataset.name}</ItemTitle>
        <ItemDescription className="text-xs">
          {dataset.description}
        </ItemDescription>
      </ItemContent>
      <ItemActions>
        <Badge
          variant="outline"
          className="text-[10px] h-5 px-1.5 bg-zinc-50 text-zinc-500 border-zinc-200"
        >
          {dataset.data_type}
        </Badge>

        {/* 이력 조회 버튼 */}
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                size="sm"
                onClick={() => handleClick(dataset.dataset_id)}
                variant="ghost"
                className="p-1 hover:bg-blue-100 hover:text-blue-600 text-zinc-400 transition-colors"
              >
                <Info className="w-3 h-3" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="top">
              <p className="text-xs">데이터 이력</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>

        {/* 업로드 버튼 */}
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                size="sm"
                onClick={() => handleClick(dataset.dataset_id)}
                variant="ghost"
                className="p-1 hover:bg-violet-100 hover:text-violet-600 text-zinc-400 transition-colors"
              >
                <Upload className="w-3 h-3" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="top">
              <p className="text-xs">파일 업로드</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>

        {/* 삭제 */}
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                size="sm"
                variant="ghost"
                className="p-1 rounded-lg hover:bg-red-50 hover:text-red-500 text-zinc-400 transition-colors"
              >
                <Trash2 className="w-3.5 h-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="top">
              <p className="text-xs">데이터셋 삭제</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </ItemActions>
      <DatasetDialog
        open={open}
        onClose={() => setOpen(false)}
        dataset={dataset}
        versions={versions}
      />
    </Item>
  );
}
