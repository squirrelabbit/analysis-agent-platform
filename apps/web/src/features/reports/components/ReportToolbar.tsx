// 보고서 에디터 상단 인페이지 툴바 — 편집/미리보기 모드 전환 + 내보내기 메뉴.
// (변경분은 자동저장되므로 초기화 버튼은 두지 않는다.)
import { useEffect, useRef, useState } from "react";
import {
  ChevronDown,
  Download,
  Eye,
  FileCode2,
  FileText,
  Pencil,
  Presentation,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { ReportMode } from "../models/editor";

export type ExportFormat = "pdf" | "html" | "pptx" | "hwp";

const EXPORT_ITEMS: {
  fmt: ExportFormat;
  title: string;
  desc: string;
  icon: React.ReactNode;
  iconClass: string;
  soon?: boolean;
}[] = [
  {
    fmt: "pdf",
    title: "PDF로 저장",
    desc: "인쇄 대화상자 · 배포·보관용",
    icon: <FileText className="h-4.25 w-4.25" />,
    iconClass: "bg-red-50 text-red-600",
  },
  {
    fmt: "html",
    title: "HTML 다운로드",
    desc: "단일 파일 · 레이아웃 그대로",
    icon: <FileCode2 className="h-4.25 w-4.25" />,
    iconClass: "bg-blue-50 text-blue-600",
  },
  {
    fmt: "pptx",
    title: "PPTX",
    desc: "블록 1개 = 슬라이드 1장",
    icon: <Presentation className="h-4.25 w-4.25" />,
    iconClass: "bg-amber-50 text-amber-600",
    soon: true,
  },
  {
    fmt: "hwp",
    title: "한글 (HWP)",
    desc: "DOCX로 내보내 한글에서 열기",
    icon: <FileText className="h-4.25 w-4.25" />,
    iconClass: "bg-violet-50 text-violet-600",
    soon: true,
  },
];

export function ReportToolbar({
  mode,
  onMode,
  onExport,
}: {
  mode: ReportMode;
  onMode: (mode: ReportMode) => void;
  onExport: (fmt: ExportFormat) => void;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!menuOpen) return;
    const onDoc = (e: MouseEvent) => {
      if (!wrapRef.current?.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener("click", onDoc);
    return () => document.removeEventListener("click", onDoc);
  }, [menuOpen]);

  const modes: { value: ReportMode; label: string; icon: React.ReactNode }[] = [
    { value: "edit", label: "편집", icon: <Pencil className="h-3.75 w-3.75" /> },
    {
      value: "preview",
      label: "미리보기",
      icon: <Eye className="h-3.75 w-3.75" />,
    },
  ];

  return (
    <div className="flex items-center gap-2.5">
      {/* 모드 전환 */}
      <div className="inline-flex rounded-xl bg-zinc-200/60 p-0.75">
        {modes.map((m) => (
          <button
            key={m.value}
            onClick={() => onMode(m.value)}
            className={cn(
              "inline-flex items-center gap-1.5 rounded-lg px-3.5 py-1.75 text-[13.5px] font-semibold transition-colors",
              mode === m.value
                ? "bg-white text-zinc-900 shadow-sm"
                : "text-zinc-500 hover:text-zinc-700",
            )}
          >
            {m.icon}
            {m.label}
          </button>
        ))}
      </div>

      <div ref={wrapRef} className="relative">
        <button
          onClick={() => setMenuOpen((v) => !v)}
          className="inline-flex h-9.5 items-center gap-1.75 rounded-xl bg-violet-600 px-3.5 text-[13.5px] font-bold text-white transition hover:brightness-105"
        >
          <Download className="h-3.75 w-3.75" />
          내보내기
          <ChevronDown
            className={cn(
              "h-3.75 w-3.75 transition-transform",
              menuOpen && "rotate-180",
            )}
          />
        </button>

        {menuOpen && (
          <div className="absolute right-0 top-11.5 z-60 w-62 rounded-2xl border border-zinc-200 bg-white p-1.5 shadow-2xl">
            <div className="px-2.5 pb-1.25 pt-2 text-[10.5px] font-extrabold tracking-wider text-zinc-400">
              바로 사용 가능
            </div>
            {EXPORT_ITEMS.filter((it) => !it.soon).map((it) => (
              <ExportRow
                key={it.fmt}
                item={it}
                onClick={() => {
                  setMenuOpen(false);
                  onExport(it.fmt);
                }}
              />
            ))}
            <div className="px-2.5 pb-1.25 pt-2 text-[10.5px] font-extrabold tracking-wider text-zinc-400">
              문서 형식
            </div>
            {EXPORT_ITEMS.filter((it) => it.soon).map((it) => (
              <ExportRow
                key={it.fmt}
                item={it}
                onClick={() => {
                  setMenuOpen(false);
                  onExport(it.fmt);
                }}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function ExportRow({
  item,
  onClick,
}: {
  item: (typeof EXPORT_ITEMS)[number];
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className="flex w-full items-center gap-2.75 rounded-xl px-2.5 py-2.25 text-left transition hover:bg-zinc-50"
    >
      <span
        className={cn(
          "grid h-8.5 w-8.5 shrink-0 place-items-center rounded-lg",
          item.iconClass,
        )}
      >
        {item.icon}
      </span>
      <span className="min-w-0">
        <b className="block text-[13.5px] font-bold text-zinc-900">
          {item.title}
        </b>
        <small className="mt-px block text-[11.5px] text-zinc-400">
          {item.desc}
        </small>
      </span>
      {item.soon && (
        <span className="ml-auto shrink-0 rounded border border-zinc-200 px-1.5 py-0.5 text-[10px] font-extrabold text-zinc-400">
          준비 중
        </span>
      )}
    </button>
  );
}
