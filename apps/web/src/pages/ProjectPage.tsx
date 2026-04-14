import DetailPanel from "@/components/projects/DetailPanel";
import ProjectHead from "@/components/projects/ProjectHead";
import ProjectItem from "@/components/projects/ProjectItem";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { useState } from "react";
const projects = [
  {
    project_id: "edc3cb2a-9fbb-4450-93fd-ac2715579206",
    name: "festival-manual",
    description: "manual test",
    created_at: "2026-04-01 14:05:07.366 +0900",
  },
  {
    project_id: "59ff81f0-0e1b-4989-8f27-2939f59341a4",
    name: "semantic-search-smoke",
    description: "semantic evidence smoke",
    created_at: "2026-04-01 16:10:05.668 +0900",
  },
  {
    project_id: "ee26254f-2908-4ed8-bbb6-b0521b954bd1",
    name: "cluster-smoke	issue",
    description: "cluster smoke",
    created_at: "2026-04-01 16:56:59.447 +0900",
  },
  {
    project_id: "0c59c074-477b-4831-b76e-e478deda71a4",
    name: "semantic-search-smoke",
    description: "semantic evidence smoke",
    created_at: "2026-04-01 16:56:59.447 +0900",
  },
  {
    project_id: "f7786e6d-c8f7-4124-9a44-5914387481cb",
    name: "semantic-search-smoke",
    description: "semantic evidence smoke",
    created_at: "2026-04-01 16:58:07.157 +0900",
  },
  {
    project_id: "c0fd2d8e-db26-4c28-8920-5ae7a7b4366f",
    name: "cluster-smoke",
    description: "issue cluster smoke",
    created_at: "2026-04-01 16:58:07.249 +0900",
  },
  {
    project_id: "f77d86e6d-c8f7-4124-9a44-5914387481cb",
    name: "semantic-search-smoke",
    description: "semantic evidence smoke",
    created_at: "2026-04-01 16:58:07.157 +0900",
  },
  {
    project_id: "c0fd2d8se-db26-4c28-8920-5ae7a7b4366f",
    name: "cluster-smoke",
    description: "issue cluster smoke",
    created_at: "2026-04-01 16:58:07.249 +0900",
  },
];

export default function ProjectPage() {
  const [selected, setSelected] = useState("c0fd2d8e-db26-4c28-8920-5ae7a7b4366f");
  
  return (
    <div className="flex h-full gap-3">
      <div className="flex-1">
        <div className="flex flex-col gap-2">
      <ProjectHead />
      <ScrollArea className="h-140 ">
        <div className="flex flex-col gap-2">
          {projects.map((item, idx) => (
            <ProjectItem
              key={idx}
              item={item}
              isActive={item.project_id === selected}
              onClick={() => setSelected(item.project_id)}
            />
          ))}
        </div>
      </ScrollArea>
    </div>
      </div>

      <Separator orientation="vertical" />
      {selected &&
      <div className="flex-2">
        <DetailPanel project={projects.find((item) => item.project_id === selected)} />
      </div>
      }
    </div>
  );
}
