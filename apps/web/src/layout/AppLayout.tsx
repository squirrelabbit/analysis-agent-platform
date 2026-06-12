import { useLocation } from "react-router-dom";
import Topbar from "./Topbar";

// Topbar 없이 전체화면으로 렌더할 경로(로그인 등 인증 화면).
const BARE_PATHS = ["/login", "/auth/callback", "/forbidden"];

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const { pathname } = useLocation();
  if (BARE_PATHS.includes(pathname)) {
    return <>{children}</>;
  }
  return (
    <div className="min-h-screen bg-[#f0f2f6]">
      <Topbar />
      <main className="mx-auto w-full overflow-hidden">
        {children}
      </main>
    </div>
  );
}
