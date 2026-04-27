import { NavLink } from "react-router-dom"
import { Database, FileText } from "lucide-react"
import type { Project } from "@/features/project/types/project"


export default function Sidebar({ project }: { project: Project}) {
  const menus = [
    {
      name: "데이터셋",
      path: `/projects/${project.id}/datasets`,
      icon: Database,
    },
    {
      name: "시나리오",
      path: `/projects/${project.id}/scenarios`,
      icon: FileText,
    },
  ]

  return (
    <aside className="w-52 border-r bg-white flex flex-col p-4 gap-4">
      <div className="text-sm font-semibold text-zinc-800 truncate">
        {project?.name || "로딩중..."}
      </div>

      <nav className="flex flex-col gap-1">
        {menus.map((menu) => {
          const Icon = menu.icon

          return (
            <NavLink
              key={menu.name}
              to={menu.path}
              // end={menu.name === "데이터셋"} // 기본 경로 active 처리
              className={({ isActive }) =>
                `
                flex items-center gap-2 px-3 py-2 rounded-md text-sm
                transition-colors
                ${
                  isActive
                    ? "bg-indigo-50 text-indigo-500 font-medium"
                    : "text-zinc-600 hover:bg-zinc-100"
                }
              `
              }
            >
              <Icon className="w-4 h-4" />
              {menu.name}
            </NavLink>
          )
        })}
      </nav>
    </aside>
  )
}