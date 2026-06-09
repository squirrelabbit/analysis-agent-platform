import { useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { Database, FileText, MessageCircle, PanelLeft } from "lucide-react";
import type { Project } from "@/features/projects/models/model";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { MOCK_RESULTS } from "@/features/reports/models/model";

export default function Sidebar({ project }: { project: Project }) {
  const { pathname } = useLocation();
  const basePath = `/projects/${project.id}`;

  // 수동 접힘 상태 + 화면이 sm 이하면 강제 접힘.
  const [collapsed, setCollapsed] = useState(false);
  const [isSmall, setIsSmall] = useState(false);

  useEffect(() => {
    // Tailwind sm 브레이크포인트(640px) 미만이면 자동 접기.
    const mq = window.matchMedia("(max-width: 639px)");
    const update = () => setIsSmall(mq.matches);
    update();
    mq.addEventListener("change", update);
    return () => mq.removeEventListener("change", update);
  }, []);

  const isCollapsed = isSmall || collapsed;

  const menus = [
    {
      name: "데이터셋",
      path: `${basePath}/datasets`,
      icon: Database,
      badge: project.datasetCount,
    },
    // {
    //   name: "시나리오",
    //   path: `${basePath}/scenarios`,
    //   icon: FileText,
    //   badge: project.scenarioCount,
    // },
    {
      name: "채팅",
      path: `${basePath}/chats`,
      icon: MessageCircle,
      badge: project.chatCount,
    },
    {
      name: "보고서",
      path: `${basePath}/reports`,
      icon: FileText,
      badge: MOCK_RESULTS.length,
    },
  ];

  return (
    <aside
      className={cn(
        "flex shrink-0 flex-col gap-4 border-r bg-white py-4 transition-[width] duration-200 ease-in-out",
        isCollapsed ? "w-16 px-2" : "w-64 px-4",
      )}
    >
      <TooltipProvider delayDuration={0}>
        {/* 헤더: 프로젝트명 + 접기/펼치기 토글 */}
        <div
          className={cn(
            "flex items-center",
            isCollapsed ? "justify-center" : "justify-between gap-2",
          )}
        >
          {!isCollapsed && (
            <div className="truncate text-sm font-semibold text-zinc-800">
              {project?.name || "로딩중..."}
            </div>
          )}
          {/* sm 이하에선 강제 접힘이라 토글 숨김 */}
          {!isSmall && (
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  onClick={() => setCollapsed((v) => !v)}
                  aria-label={collapsed ? "사이드바 펼치기" : "사이드바 접기"}
                  className="grid h-8 w-8 shrink-0 place-items-center rounded-md text-zinc-500 transition-colors hover:bg-zinc-100 hover:text-zinc-800"
                >
                  <PanelLeft className="h-4 w-4" />
                </button>
              </TooltipTrigger>
              <TooltipContent side="right">
                {collapsed ? "펼치기" : "접기"}
              </TooltipContent>
            </Tooltip>
          )}
        </div>

        <nav className="flex flex-col gap-1">
          {menus.map((menu) => {
            const Icon = menu.icon;
            const active =
              pathname === menu.path || pathname.startsWith(`${menu.path}/`);

            const link = (
              <NavLink
                to={menu.path}
                className={cn(
                  "flex items-center rounded-md text-sm transition-colors",
                  isCollapsed
                    ? "justify-center px-0 py-2.5"
                    : "justify-between gap-2 px-3 py-2",
                  active
                    ? "bg-violet-100 text-violet-600 font-medium"
                    : "text-zinc-600 hover:bg-zinc-100",
                )}
              >
                <div
                  className={cn("flex items-center", !isCollapsed && "gap-2")}
                >
                  <Icon className="h-4 w-4 shrink-0" />
                  {!isCollapsed && menu.name}
                </div>
                {!isCollapsed && (
                  <Badge
                    className={cn(
                      active
                        ? "bg-violet-200 text-violet-600"
                        : "bg-zinc-200 text-zinc-600",
                    )}
                  >
                    {menu.badge}
                  </Badge>
                )}
              </NavLink>
            );

            // 접힘 상태에선 아이콘만 보이므로 hover 시 페이지명 tooltip 노출.
            return (
              <div key={menu.name}>
                {isCollapsed ? (
                  <Tooltip>
                    <TooltipTrigger asChild>{link}</TooltipTrigger>
                    <TooltipContent side="right">{menu.name}</TooltipContent>
                  </Tooltip>
                ) : (
                  link
                )}
              </div>
            );
          })}
        </nav>
      </TooltipProvider>
    </aside>
  );
}
