import { Input } from "@/components/ui/input";
import ProjectHeader from "@/features/project/components/ProjectHeader";
import ProjectList from "@/features/project/components/ProjectList";
import ProjectListSkeleton from "@/features/project/components/ProjectListSkeleton";
import { useProjects } from "@/features/project/hooks/useProjects";
import { Search } from "lucide-react";
import { useState } from "react";

export default function ProjectPage() {
  const { data: projects = [], isLoading } = useProjects();

  const [search, setSearch] = useState("");
  const filtered = projects.filter(
    ({ name, description }) =>
      name.toLowerCase().trim().includes(search.toLowerCase().trim()) ||
      description.toLowerCase().trim().includes(search.toLowerCase().trim()),
  );

  return (
    <div className="p-8">
      <ProjectHeader />
      <div className="flex justify-between items-center py-3">
        <div className="relative w-100">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-400" />
          <Input
            placeholder="Search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-9 pl-8 text-xs rounded-md border-zinc-200 bg-white focus-visible:ring-indigo-300"
          />
        </div>
        <p className="text-xs text-zinc-400">
          <span className="text-indigo-500 font-medium">
            {filtered.length}개
          </span>
          {" / "}전체 {projects.length}개
        </p>
      </div>
      {isLoading ? (
        <ProjectListSkeleton />
      ) : (
        <ProjectList total={projects.length} projects={filtered} />
      )}
    </div>
  );
}
