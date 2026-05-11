import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Play } from "lucide-react";

export default function AnalysisDialog({
  title,
  formId,
  children,
}: {
  title: string;
  formId: string;
  children: (close: () => void) => React.ReactNode
}) {
  const [open, setOpen] = useState(false);

  const close = () => setOpen(false)

  return (
    <Dialog open={open} onOpenChange={setOpen} >
      <DialogTrigger asChild>
        <Button variant="outline" className="text-xs">
          <Play className="w-3.5 h-3.5" />
          실행
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md flex flex-col max-h-[80vh]">
        <DialogHeader className="shrink-0">
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          {children(close)}
        </div>
        <DialogFooter className="flex gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            취소
          </Button>
          <Button type="submit" form={formId}>
            실행
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
