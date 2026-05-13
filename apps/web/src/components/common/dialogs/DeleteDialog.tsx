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
import { Trash2 } from "lucide-react";

interface DeleteDialgProps {
  title: string;
  onDelete: () => void;
  children?: React.ReactNode;
  Icon?: React.ReactNode;
}

export default function DeleteDialog({
  title,
  onDelete,
  children,
  Icon,
}: DeleteDialgProps) {
  return (
    <Dialog>
      <DialogTrigger>
        <Button
          variant="ghost"
          className="hover:bg-red-50 hover:text-red-500 text-zinc-400"
        >
          {Icon ? Icon : <Trash2 />}
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
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
