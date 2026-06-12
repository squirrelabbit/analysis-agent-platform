import { apiClient } from "@/api/client";

// 인증 API (ADR-025). 세션 쿠키(asp_session) 기반 — apiClient.withCredentials로 전송.

export interface AuthUser {
  user_id: string;
  email: string;
  name?: string;
  avatar_url?: string;
  global_role: string; // admin | user
  status: string; // active | disabled
}

export interface AuthMe {
  user: AuthUser;
  project_roles: Record<string, string>; // project_id → viewer|editor|owner
}

export interface AuthConfig {
  auth_enabled: boolean;
  provider: string; // "google"
}

export const authApi = {
  async getMe(): Promise<AuthMe> {
    const { data } = await apiClient.get<AuthMe>("/auth/me");
    return data;
  },
  async getConfig(): Promise<AuthConfig> {
    const { data } = await apiClient.get<AuthConfig>("/auth/config");
    return data;
  },
  async logout(): Promise<void> {
    await apiClient.post("/auth/logout");
  },
};
