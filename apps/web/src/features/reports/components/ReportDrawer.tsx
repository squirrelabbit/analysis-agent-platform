import { useState } from "react";
import {
  BarChart3,
  Code2,
  Download,
  FileText,
  GripVertical,
  Table2,
  X,
} from "lucide-react";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { cn } from "@/lib/utils";
import type { ReportFormat, ReportResult } from "../models/model";

const FORMATS: { value: ReportFormat; label: string; icon: typeof FileText }[] = [
  { value: "pdf", label: "PDF", icon: FileText },
  { value: "html", label: "HTML", icon: Code2 },
  { value: "xlsx", label: "Excel", icon: Table2 },
];

// 선택 항목을 정리하고 형식을 골라 내려받는 보고서 구성 패널.
export function ReportDrawer({
  open,
  onOpenChange,
  items,
  onRemove,
  onReorder,
  onDownload,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  items: ReportResult[];
  onRemove: (id: string) => void;
  onReorder: (from: number, to: number) => void;
  onDownload: (format: ReportFormat) => void;
}) {
  const [format, setFormat] = useState<ReportFormat>("pdf");
  // 드래그 정렬 상태: 잡은 행(dragIndex)과 올려둔 행(overIndex).
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [overIndex, setOverIndex] = useState<number | null>(null);
  const resetDrag = () => {
    setDragIndex(null);
    setOverIndex(null);
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="flex w-full flex-col gap-0 bg-zinc-50 p-0 sm:max-w-105"
      >
        <SheetHeader className="border-b border-zinc-100 bg-white px-5 py-4">
          <SheetTitle className="text-[17px] font-extrabold">
            보고서 구성
          </SheetTitle>
          <SheetDescription className="text-xs text-zinc-400">
            순서를 정리하고 형식을 선택해 내려받으세요.
          </SheetDescription>
        </SheetHeader>

        {/* 선택 항목 목록 */}
        <div className="flex flex-1 flex-col gap-2 overflow-y-auto px-4 py-4">
          {items.length === 0 ? (
            <p className="py-16 text-center text-sm text-zinc-400">
              선택된 항목이 없습니다.
            </p>
          ) : (
            items.map((it, idx) => (
              <div
                key={it.id}
                draggable
                onDragStart={() => setDragIndex(idx)}
                onDragEnter={() => setOverIndex(idx)}
                onDragOver={(e) => e.preventDefault()}
                onDragEnd={resetDrag}
                onDrop={() => {
                  if (dragIndex !== null && dragIndex !== idx)
                    onReorder(dragIndex, idx);
                  resetDrag();
                }}
                className={cn(
                  "flex items-center gap-2.5 rounded-xl border bg-white px-3 py-2.5 shadow-sm transition-all",
                  dragIndex === idx
                    ? "opacity-40"
                    : overIndex === idx && dragIndex !== null
                      ? "border-violet-400 ring-2 ring-violet-100"
                      : "border-zinc-100",
                )}
              >
                <GripVertical className="h-4 w-4 shrink-0 cursor-grab text-zinc-300 active:cursor-grabbing" />
                <span
                  className={cn(
                    "grid h-8 w-8 shrink-0 place-items-center rounded-[9px]",
                    it.kind === "chart"
                      ? "bg-violet-50 text-violet-700"
                      : "bg-blue-50 text-blue-600",
                  )}
                >
                  {it.kind === "chart" ? (
                    <BarChart3 className="h-4 w-4" />
                  ) : (
                    <Table2 className="h-4 w-4" />
                  )}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="truncate text-[13px] font-bold text-zinc-800">
                    {it.title}
                  </div>
                  <div className="truncate text-[11px] text-zinc-400">
                    {it.chat}
                  </div>
                </div>
                <button
                  onClick={() => onRemove(it.id)}
                  className="grid h-6 w-6 shrink-0 place-items-center rounded-md text-zinc-300 transition-colors hover:bg-red-50 hover:text-red-500"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            ))
          )}
        </div>

        {/* 형식 + 다운로드 */}
        <div className="border-t border-zinc-100 bg-white px-4 py-4">
          <div className="mb-2.5 text-xs font-bold text-zinc-600">
            다운로드 형식
          </div>
          <div className="grid grid-cols-3 gap-2">
            {FORMATS.map(({ value, label, icon: Icon }) => (
              <button
                key={value}
                onClick={() => setFormat(value)}
                className={cn(
                  "flex flex-col items-center gap-1.5 rounded-xl border-[1.5px] py-3 transition-colors",
                  format === value
                    ? "border-violet-500 bg-violet-50 text-violet-700"
                    : "border-zinc-200 bg-white text-zinc-500 hover:border-zinc-300",
                )}
              >
                <Icon className="h-5 w-5" strokeWidth={1.7} />
                <b className="text-xs font-bold">{label}</b>
              </button>
            ))}
          </div>
          <button
            disabled={items.length === 0}
            onClick={() => onDownload(format)}
            className="mt-3.5 flex h-12 w-full items-center justify-center gap-2 rounded-xl bg-zinc-900 text-[14.5px] font-bold text-white transition-opacity hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <Download className="h-4.5 w-4.5" />
            보고서 다운로드 ({items.length})
          </button>
        </div>
      </SheetContent>
    </Sheet>
  );
}
