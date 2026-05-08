import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Trash2 } from "lucide-react";

export default function DeleteDialog({
  onDelete,
  title,
  children,
}: {
  title: string,
  onDelete: () => void;
  children?: React.ReactNode;
}) {
  return (
    <Dialog>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <DialogTrigger asChild>
              <Button
                size="icon-xs"
                variant="ghost"
                className="rounded-lg hover:bg-red-50 hover:text-red-500 text-zinc-400"
              >
                <Trash2  />
              </Button>
            </DialogTrigger>
          </TooltipTrigger>
          <TooltipContent side="top">
            <p className="text-xs">삭제</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>{title} 삭제</DialogTitle>
          <DialogDescription className="text-xs">
            정말 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.
          </DialogDescription>
          {children}
        </DialogHeader>

        <DialogFooter className="flex gap-2">
          <DialogClose asChild>
            <Button variant="outline">취소</Button>
          </DialogClose>
          <DialogClose asChild>
            <Button variant="destructive" onClick={onDelete}>
              삭제
            </Button>
          </DialogClose>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
