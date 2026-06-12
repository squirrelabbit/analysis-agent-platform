import { useState } from "react";
import { LogOut } from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuItem,
} from "@/components/ui/dropdown-menu";
import { useAuthMe } from "../hooks/auth.query";
import { authApi } from "../api/auth.api";

// 헤더 우측 프로필 메뉴 (ADR-025). 로그인 상태면 아바타+이름, 드롭다운에 로그아웃.
// 미로그인(401)/auth 비활성 등 user가 없으면 아무것도 렌더하지 않는다.

function initials(name?: string, email?: string): string {
  const src = (name || email || "").trim();
  if (!src) return "?";
  return src.charAt(0).toUpperCase();
}

export default function UserMenu() {
  const { data, isError } = useAuthMe();
  const queryClient = useQueryClient();
  const [loading, setLoading] = useState(false);

  if (isError || !data?.user) return null;
  const user = data.user;
  const label = user.name || user.email;

  const handleLogout = async () => {
    setLoading(true);
    try {
      await authApi.logout();
    } catch {
      // 로그아웃 실패해도 세션 쿠키는 만료 처리되므로 로그인 화면으로 이동.
    }
    queryClient.clear();
    window.location.href = "/login";
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="flex items-center gap-2 rounded-full py-1 pl-1 pr-2 outline-none hover:bg-[#f0f2f6] focus-visible:ring-2 focus-visible:ring-ring">
        <Avatar size="sm">
          {user.avatar_url && (
            <AvatarImage src={user.avatar_url} alt={label} referrerPolicy="no-referrer" />
          )}
          <AvatarFallback>{initials(user.name, user.email)}</AvatarFallback>
        </Avatar>
        <span className="max-w-[140px] truncate text-sm text-[#3a414e]">{label}</span>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-60">
        <DropdownMenuLabel className="font-normal">
          <div className="flex flex-col gap-0.5">
            <span className="truncate text-sm font-semibold">{user.name || "이름 없음"}</span>
            <span className="truncate text-xs text-muted-foreground">{user.email}</span>
            {user.global_role === "admin" && (
              <span className="mt-1 w-fit rounded bg-[#fdecdd] px-1.5 py-0.5 text-[11px] font-semibold text-[#d95e15]">
                관리자
              </span>
            )}
          </div>
        </DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          variant="destructive"
          disabled={loading}
          onSelect={(e) => {
            e.preventDefault();
            void handleLogout();
          }}
        >
          <LogOut />
          로그아웃
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
