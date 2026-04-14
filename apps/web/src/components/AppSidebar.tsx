import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { FolderOpen, MessageCircle, Settings, Sparkle } from "lucide-react";
import { Link, useLocation } from "react-router-dom";
export function AppSidebar() {
  const menus = [
    { label: "채팅", icon: MessageCircle, href: "/chats" },
    { label: "프로젝트", icon: FolderOpen, badge: "6", href: "/projects" },
  ];
  const location = useLocation();
  return (
    <Sidebar collapsible={"icon"}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild className="h-12 p-2 gap-2">
              <a href="#">
                <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
                  <Sparkle />
                </div>
                <div className="flex flex-col">
                  <div>Analysis</div>
                  <div>Agent Platform</div>
                </div>
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>메뉴</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {menus.map((menu) => (
                <SidebarMenuItem key={menu.label} className="items-center">
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === menu.href}
                    className="h-10 hover:bg-muted data-[active=true]:bg-primary/10 data-[active=true]:text-primary"
                  >
                    <Link to={menu.href}>
                      <menu.icon className="w-4 h-4" />
                      {menu.label}
                      {menu.badge && (
                        <SidebarMenuBadge className="ml-auto min-w-5 h-5 px-1 flex items-center justify-center rounded-full bg-primary text-white text-xs">
                          {menu.badge}
                        </SidebarMenuBadge>
                      )}
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem className="flex justify-end">
            <SidebarMenuButton>
              <Settings />
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}
