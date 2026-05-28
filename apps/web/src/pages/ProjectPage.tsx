import ProjectContainer from "@/features/projects/components/ProjectContainer";
import { useProjects } from "@/features/projects/hooks/project.query";

export default function ProjectPage() {
  const { data: projects = [], isLoading } = useProjects();

  return (
    <div className="p-8">
      <ProjectContainer projects={projects} isLoading={isLoading} />
    </div>
  );
}
