import { SidebarProvider } from "@/components/ui/sidebar"
import Topbar from "./Topbar"
import { AppSidebar } from "../components/AppSidebar"

export default function AppLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <SidebarProvider>
      <div className="w-full h-screen flex">

        {/* Sidebar */}
        <AppSidebar />

        {/* 오른쪽 영역 */}
        <div className="flex flex-col flex-1">

          {/* Topbar */}
          <Topbar />

          {/* Content */}
          <main className="flex-1 overflow-y-auto p-4">
            {children}
          </main>

        </div>

      </div>
    </SidebarProvider>
  )
}