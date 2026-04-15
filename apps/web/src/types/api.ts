// 공통 에러
export interface ApiError {
  status: number
  code: string        // 'NOT_FOUND' | 'UNAUTHORIZED' 등
  message: string
}