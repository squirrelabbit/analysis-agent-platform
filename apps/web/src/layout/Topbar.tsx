import { FolderOpen, MessageCircle, Sparkle } from "lucide-react";
import { NavLink } from "react-router-dom";

export default function Topbar() {
  const menus = [
    { label: "채팅", icon: MessageCircle, href: "/chats" },
    { label: "프로젝트", icon: FolderOpen, href: "/projects" },
    // { label: "보고서", icon: FolderOpen, href: "/reports" },
  ];

  const navClassName = ({ isActive }: { isActive: boolean }) =>
    `rounded-md px-3 py-2 text-sm ${isActive ? "bg-indigo-50 text-indigo-500" : "text-[#4b5270] hover:bg-[#edeef2]"}`;

  return (
    <header className=" border-b border-[#e2e5ed] bg-white">
      <div className="mx-3 flex h-full  items-center gap-6">
        <a href="#" className="flex gap-2 py-1 items-center">
          <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
            <Sparkle />
          </div>
          <div className="flex flex-col text-sm">
            <div>Analysis </div>
            <div>Agent Platform</div>
          </div>
        </a>
        <nav className="flex gap-1">
          {menus.map((menu) => (
            <NavLink key={menu.label} className={navClassName} to={menu.href}>
              <div className="flex items-center gap-1.5">
                <menu.icon className="w-4 h-4" />
                {menu.label}
              </div>
            </NavLink>
          ))}
        </nav>
      </div>
    </header>
  );
}
