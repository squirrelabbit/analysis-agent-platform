import { Sparkle } from "lucide-react";

export default function Topbar() {
  return (
    <header className=" border-b border-[#e2e5ed] bg-white">
      <div className="mx-3 flex h-full  items-center gap-6">
        <a href="/projects" className="flex gap-2 py-1 items-center">
          <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
            <Sparkle size={18} />
          </div>
          <div className="flex flex-col text-sm">
            <div>Analysis </div>
            <div>Agent Platform</div>
          </div>
        </a>
      </div>
    </header>
  );
}
