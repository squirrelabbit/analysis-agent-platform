export type Dataset = {
  
    id: string,
    projectId: string,
    name: string,
    type: string,
    size: string,
    version: number,
    createdAt: string,
    updatedAt: string,
}

// ── Mock data (replace with real API) ────────────────────────────────────────
export const MOCK_DATASETS: Dataset[] = [
  {
    id: '1',
    projectId: 'p1',
    name: 'festival_data.csv',
    type: 'csv',
    size: '1.2MB',
    version: 3,
    createdAt: '2025-01-02',
    updatedAt: '2025-01-14',
  },
  {
    id: '2',
    projectId: 'p1',
    name: 'channel_mapping.xlsx',
    type: 'xlsx',
    size: '540KB',
    version: 1,
    createdAt: '2025-01-08',
    updatedAt: '2025-01-08',
  },
  {
    id: '3',
    projectId: 'p1',
    name: 'customer_data.json',
    type: 'json',
    size: '1.1MB',
    version: 2,
    createdAt: '2025-01-05',
    updatedAt: '2025-01-10',
  },
]

export const MOCK_HISTORY: Record<string, { ver: string; desc: string; date: string; isCurrent: boolean }[]> = {
  '1': [
    { ver: 'v3', desc: '12월 4주차 데이터 추가, 컬럼 정규화', date: '2025.01.14', isCurrent: true },
    { ver: 'v2', desc: '11월 결측치 보완, 이상치 제거', date: '2025.01.08', isCurrent: false },
    { ver: 'v1', desc: '최초 업로드 — 10~11월 원본 데이터', date: '2025.01.02', isCurrent: false },
  ],
  '2': [
    { ver: 'v1', desc: '최초 업로드', date: '2025.01.08', isCurrent: true },
  ],
  '3': [
    { ver: 'v2', desc: '고객 ID 정규화 및 중복 제거', date: '2025.01.10', isCurrent: true },
    { ver: 'v1', desc: '최초 업로드', date: '2025.01.05', isCurrent: false },
  ],
}