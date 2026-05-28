import axios from 'axios'

export const apiClient = axios.create({
  // dev: vite proxy(/api) 경유로 CORS 회피, prod: 빌드 시 주입된 절대주소
  baseURL: import.meta.env.DEV ? '/api' : import.meta.env.VITE_API_BASE_URL,
  timeout: 10_000,
  // headers: { 'Content-Type': 'application/json' },
})

// 요청 인터셉터 — 토큰 주입 (추후 회원 관리 시)
// apiClient.interceptors.request.use((config) => {
//   const token = localStorage.getItem('token')
//   if (token) config.headers.Authorization = `Bearer ${token}`
//   return config
// })

// 응답 인터셉터 — 에러 정규화
// apiClient.interceptors.response.use(
//   (res) => res,
//   (error) => {
//     const apiError: ApiError = {
//       status: error.response?.status ?? 0,
//       code: error.response?.data?.code ?? 'UNKNOWN',
//       message: error.response?.data?.message ?? '알 수 없는 오류가 발생했습니다',
//     }
//     return Promise.reject(apiError)
//   }
// )