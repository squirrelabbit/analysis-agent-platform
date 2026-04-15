import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { FileText } from "lucide-react";
import { FileRow } from "./FileRow";
import FileUpload from "@/components/common/FileUpload";
import type { Dataset } from "@/types";

type DatasetDialogProps = {
  open: boolean;
  onClose: () => void;
  dataset: Dataset
};

const files = [
  {
    filename: "customer_master_v3.csv",
    byte_size: 124,
    uploaded_at: "2025-06-10",
  },
  {
    filename: "customer_mster_v3.json",
    byte_size: 124,
    uploaded_at: "2025-06-10",
  },
  {
    filename: "customer_mster_v3.md",
    byte_size: 124,
    uploaded_at: "2025-06-10",
  },
];

export function DatasetDialog({ open, onClose, dataset }: DatasetDialogProps) {
  return (
    <div>
      <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
        <DialogContent className="sm:max-w-lg gap-0 p-0 overflow-hidden rounded-2xl border-zinc-200">
          <DialogHeader className="px-6 pt-6 pb-4 border-b border-zinc-100">
            <div className="flex items-center gap-2.5">
              <div className="w-8 h-8 rounded-xl bg-violet-100 flex items-center justify-center">
                <FileText className="w-4 h-4 text-violet-600" />
              </div>
              <div>
                <DialogTitle className="text-sm font-semibold text-zinc-800">
                  {dataset.name}
                </DialogTitle>
                <DialogDescription className="text-xs">
                  {dataset.data_type}
                </DialogDescription>
              </div>
            </div>
          </DialogHeader>

          <div className="px-3 py-5">
            <Tabs defaultValue="dataset">
              <TabsList>
                <TabsTrigger value="version">데이터 버전</TabsTrigger>
                <TabsTrigger value="add">데이터 등록</TabsTrigger>
              </TabsList>
              <TabsContent value="version">
                {files.length === 0 ? (
                  /* 파일 없는 빈 상태 */
                  <FileUpload title="등록된 파일이 없습니다" />
                ) : (
                  <div className="px-3 py-2 space-y-1">
                    {files.map((file, idx) => (
                      <FileRow
                        key={idx}
                        file={file}
                        isLatest={idx === 0}
                      />
                    ))}
                  </div>
                )}
              </TabsContent>
              <TabsContent value="add">
                <FileUpload />
              </TabsContent>
            </Tabs>
          </div>

          <DialogFooter className="px-6 pb-6 gap-2 flex-row">
            <DialogClose asChild>
              <Button variant="outline" onClick={onClose}>
                취소
              </Button>
            </DialogClose>
            <Button>
              등록
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
