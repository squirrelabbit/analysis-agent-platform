import { useParams } from "react-router-dom";

export function useProjectId() {
  const { projectId } = useParams();
  if (!projectId) {
    throw new Error("projectId 없음");
  }

  return {
    projectId,
  };
}
