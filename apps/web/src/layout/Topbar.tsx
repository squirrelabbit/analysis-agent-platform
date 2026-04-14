import { PanelLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useSidebar } from "@/components/ui/sidebar";

export default function Topbar() {
  const { toggleSidebar } = useSidebar();

  return (
    <header className="h-14 border-b flex items-center justify-between px-4 gap-2">
      <Button variant="ghost" size="icon" onClick={toggleSidebar}>
        <PanelLeft onClick={toggleSidebar} />
      </Button>
      <Button>
        + 새 채팅
      </Button>
    </header>
  );
}
