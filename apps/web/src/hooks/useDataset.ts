import { useState, useEffect } from 'react'
import { datasetsApi } from '@/api/dataset'
import type { Dataset } from '@/types'
import type { CreateDatasetPayload } from '@/types/dto/dataset.dto'

export function useDataset(projectId: string) {
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [dataset, setDataset] = useState<Dataset>()
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)


  // 목록 조회
  async function fetchDatasets() {
    setIsLoading(true)
    setError(null)
    try {
      const res = await datasetsApi.getAll(projectId)
      setDatasets(res)
    } catch (err: any) {
      setError(err.message)
    } finally {
      setIsLoading(false)
    }
  }

  // 생성
  async function addDataset(payload: CreateDatasetPayload) {
    try {
      const res = await datasetsApi.create(projectId, payload)
      setDatasets((prev) => [res, ...prev])
    } catch (err: any) {
      setError(err.message)
    }
  }
  
  // 데이터셋 조회
  async function findDatasetById(datasetId: string) {
    try {
      const res = await datasetsApi.getById(projectId, datasetId)
      setDataset(res)
    } catch (err: any) {
      setError(err.message)
    } finally {
      // setIsLoading(false)
    }
  }

  // 최초 마운트
  useEffect(() => {
    fetchDatasets()
  }, [])

  return {
    datasets,
    isLoading,
    error,
    dataset,
    addDataset,
    findDatasetById,
  }
}