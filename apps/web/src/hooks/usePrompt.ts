import { promptsApi } from "@/api/prompt";
import type { CreatePromptPayload, PromptResponse } from "@/types/dto/prompt.dto";
import { useEffect, useState } from "react";

export function usePrompt(projectId: string) {
  const [prompts, setPrompts] = useState<PromptResponse[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 프롬프트 목록 조회 
  async function fetchPrompts() {
    setIsLoading(true);
    setError(null);
    try {
      const res = await promptsApi.getAll(projectId);
      setPrompts(res);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  }

  async function addPrompt(payload: CreatePromptPayload) {
    try {
      const res = await promptsApi.create(projectId, payload);
      setPrompts((prev) => [res, ...prev]);
    } catch (err: any) {
      setError(err.message);
    }
  }

  // 최초 마운트
  useEffect(() => {
    fetchPrompts();
  }, []);

  return { prompts, isLoading, error, addPrompt };
}
