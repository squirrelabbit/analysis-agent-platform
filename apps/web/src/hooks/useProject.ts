import { useState, useEffect, useMemo } from "react";
import { projectsApi } from "@/api/project";
import type { Project } from "@/types";
import type {
  CreateProjectPayload,
  ProjectResponse,
} from "@/types/dto/project.dto";

export function useProjects() {
  const [projects, setProjects] = useState<ProjectResponse[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const selectedProject = 
    projects?.find((p) => p.project_id === selectedId) ?? null;

  const filtered = useMemo(() => {
    if (!projects) return []
    return projects.filter(
      ({ name, description }) =>
        name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
        description?.toLowerCase().includes(searchQuery.toLowerCase()),
    );
  }, [projects, searchQuery]);

  // 프로젝트 목록 조회
  async function fetchProjects() {
    setIsLoading(true);
    setError(null);
    try {
      const res = await projectsApi.getAll();
      setProjects(res);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  }

  // 프로젝트 생성
  async function addProject(payload: CreateProjectPayload) {
    try {
      const res = await projectsApi.create(payload);
      setProjects((prev) => [res, ...prev]);
      setSelectedId(res.project_id);
    } catch (err: any) {
      setError(err.message);
    }
  }

  function selectProject(project: Project) {
    setSelectedId(project.project_id);
  }

  // 검색어 바뀔 때마다 재조회
  // useEffect(() => {
  //   setFiltered(
  //     projects.filter(
  //       ({ name, description }) =>
  //         name.toLowerCase().includes(searchQuery.toLocaleLowerCase()) ||
  //         description.toLowerCase().includes(searchQuery.toLocaleLowerCase()),
  //     ),
  //   );
  // }, [projects, searchQuery]);

  // 최초 마운트
  useEffect(() => {
    fetchProjects();
  }, []);

  return {
    projects,
    filtered,
    selectedProject,
    selectedId,
    searchQuery,
    isLoading,
    error,
    setSearchQuery,
    addProject,
    selectProject,
  };
}
