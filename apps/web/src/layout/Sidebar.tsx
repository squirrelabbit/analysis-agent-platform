import { NavLink, useLocation } from "react-router-dom";
import { Database, FileText, MessageCircle } from "lucide-react";
import type { Project } from "@/features/projects/models/model";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { MOCK_RESULTS } from "@/features/reports/models/model";

export default function Sidebar({ project }: { project: Project }) {
  const { pathname } = useLocation();
  const basePath = `/projects/${project.id}`;

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

  // const subMenus = [
  //   {
  //     name: "데이터셋 버전",
  //     path: `${basePath}/datasets/${datasetId}/versions`,
  //     icon: TextSearch,
  //   },
  //   {
  //     name: "프롬프트",
  //     path: `${basePath}/datasets/${datasetId}/prompts`,
  //     icon: Braces,
  //   },
  // ]

  return (
    <aside className="w-64 border-r bg-white flex flex-col p-4 gap-4">
      <div className="text-sm font-semibold text-zinc-800 truncate">
        {project?.name || "로딩중..."}
      </div>

      <nav className="flex flex-col gap-1">
        {menus.map((menu) => {
          const Icon = menu.icon;
          return (
            <div key={menu.name}>
              <NavLink
                to={menu.path}
                className={() =>
                  cn(
                    "flex items-center gap-2 justify-between px-3 py-2 rounded-md text-sm transition-colors ",
                    pathname === menu.path ||
                      pathname.startsWith(`${menu.path}/`)
                      ? "bg-violet-100 text-violet-600 font-medium"
                      : "text-zinc-600 hover:bg-zinc-100",
                  )
                }
              >
                <div className="flex items-center gap-2">
                  <Icon className="w-4 h-4" />
                  {menu.name}
                </div>
                <Badge
                  className={cn(
                    pathname === menu.path ||
                      pathname.startsWith(`${menu.path}/`)
                      ? "bg-violet-200 text-violet-600 "
                      : "bg-zinc-200 text-zinc-600",
                  )}
                >
                  {menu.badge}
                </Badge>
              </NavLink>

              {/* datasetId 있을 때만 서브메뉴 노출 */}
              {/* {menu.name === "데이터셋" && datasetId && (
                <div className="ml-3 mt-0.5 flex flex-col gap-0.5 border-l border-zinc-100 pl-3">
                  {subMenus.map((sub) => {
                    const SubIcon = sub.icon
                    return (
                      <NavLink
                        key={sub.name}
                        to={sub.path}
                        className={({ isActive }) =>
                          cn(
                            "flex items-center gap-2 px-2 py-1.5 rounded-md text-xs transition-colors",
                            isActive
                              ? "text-indigo-500 font-medium bg-indigo-50"
                              : "text-zinc-500 hover:bg-zinc-100 hover:text-zinc-700"
                          )
                        }
                      >
                        <SubIcon className="w-3.5 h-3.5" />
                        {sub.name}
                      </NavLink>
                    )
                  })}
                </div>
              )} */}
            </div>
          );
        })}
      </nav>
    </aside>
  );
}
