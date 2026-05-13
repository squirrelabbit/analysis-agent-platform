import ProjectContainer from "@/features/project/components/ProjectContainer";
import { useProjects } from "@/features/project/hooks/project.query";

export default function ProjectPage() {
  const { data: projects = [], isLoading } = useProjects();

  return (
    <div className="p-8">
      <ProjectContainer projects={projects} isLoading={isLoading} />
    </div>
  );
}
