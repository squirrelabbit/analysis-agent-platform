import Topbar from "./Topbar";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-[#f0f2f6]">
      <Topbar />
      <main className="mx-auto w-full overflow-hidden">
        {children}
      </main>
    </div>
  );
}
