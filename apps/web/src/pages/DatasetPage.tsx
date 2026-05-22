import { Input } from "@/components/ui/input";
import DatasetCard from "@/features/dataset/components/dataset/DatasetCard";
import DatasetHeader from "@/features/dataset/components/dataset/DatasetHeader";
import DatasetTable from "@/features/dataset/components/dataset/DatasetTable";
import { useDataset } from "@/features/dataset/hooks/useDataset";
import type { Project } from "@/features/project/types/project";
import { Search } from "lucide-react";
import { useMemo, useState } from "react";
import { useOutletContext } from "react-router-dom";

export default function DatasetPage() {
  const { project } = useOutletContext<{ project: Project }>();
  const { data: datasets = [] } = useDataset(project.id);
  const [search, setSearch] = useState<string>("");

  const filtered = useMemo(
    () =>
      datasets.filter(
        ({ name, description }) =>
          name.toLowerCase().includes(search.trim().toLowerCase()) ||
          description.toLowerCase().includes(search.trim().toLowerCase()),
      ),
    [datasets],
  );
  return (
    <div className="p-8 flex flex-col gap-4">
      {project && <DatasetHeader project={project} />}
      <DatasetCard datasets={datasets} />
      <div className="flex flex-col gap-1">
        {/* 검색 */}
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="검색…"
            className="pl-7 h-8 text-[12px] bg-white"
          />
        </div>
        <DatasetTable datasets={filtered} />
      </div>
    </div>
  );
}
