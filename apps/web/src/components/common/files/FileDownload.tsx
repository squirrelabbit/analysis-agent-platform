import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "../../ui/tooltip";
import { Button } from "../../ui/button";
import { Download } from "lucide-react";

export default function FileDownload({ onClick }: { onClick: () => void }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            onClick={(e) => {
              e.stopPropagation();
              onClick();
            }}
            size="icon-xs"
            variant="ghost"
            className="rounded-lg hover:bg-zinc-200 hover:text-zinc-500 text-zinc-400"
          >
            <Download />
          </Button>
        </TooltipTrigger>
        <TooltipContent side="top">
          <p className="text-xs">다운로드</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
