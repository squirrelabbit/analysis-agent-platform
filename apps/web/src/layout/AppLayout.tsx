import { Outlet, useLocation } from "react-router-dom";
import Topbar from "./Topbar";

// Topbar 없이 전체화면으로 렌더할 경로(로그인 등 인증 화면).
const BARE_PATHS = ["/login", "/auth/callback", "/forbidden"];

export default function AppLayout() {
  const { pathname } = useLocation();
  if (BARE_PATHS.includes(pathname)) {
    return <Outlet />;
  }
  return (
    <div className="min-h-screen bg-[#f0f2f6]">
      <Topbar />
      <main className="mx-auto w-full overflow-hidden">
        <Outlet />
      </main>
    </div>
  );
}
