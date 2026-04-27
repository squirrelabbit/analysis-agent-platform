import type { Dataset } from "../types/dataset";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Button } from "@/components/ui/button";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import { Database, Info, Upload } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";

interface Props {
  datasets: Dataset[];
}

export default function DatasetList({ datasets }: Props) {
  return (
    <div className="flex flex-col gap-2">
      {datasets.map((d) => (
        <Item
          variant="outline"
          className="  hover:bg-violet-50 hover:border-violet-200 transition-colors "
        >
          <ItemMedia className="w-8 h-8 rounded-lg bg-violet-100 ">
            <Database className="w-4 h-4 text-violet-600" />
          </ItemMedia>

          <ItemContent>
            <ItemTitle>{d.name}</ItemTitle>
            <ItemDescription className="text-xs">
              {d.description}
            </ItemDescription>
          </ItemContent>
          <ItemActions>
            <Badge
              variant="outline"
              className="text-[10px] h-5 px-1.5 bg-zinc-50 text-zinc-500 border-zinc-200"
            >
              {d.dataType}
            </Badge>

            {/* 이력 조회 버튼 */}
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    size="sm"
                    // onClick={() => handleClick(d.dataset_id)}
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
                    // onClick={() => handleClick(d.dataset_id)}
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
            <DeleteDialog title="데이터셋" onDelete={() => {}} />
          </ItemActions>
          {/* <DatasetDialog
        open={open}
        onClose={() => setOpen(false)}
        dataset={dataset}
        versions={versions}
        uploadDataFile={uploadDataFile}
      /> */}
        </Item>
      ))}
    </div>
  );
}
