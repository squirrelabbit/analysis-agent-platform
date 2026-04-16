import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatFileSize(size: number) {
  if (size < 1024) return size + " B"
  if (size < 1024 * 1024) return (size / 1024).toFixed(1) + " KB"
  if (size < 1024 * 1024 * 1024) return (size / 1024 / 1024).toFixed(1) + " MB"
  return (size / 1024 / 1024 / 1024).toFixed(1) + " GB"
}

export function formatDate(timestamp: number) {
  const date = new Date(timestamp)

  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, "0")
  const day = String(date.getDate()).padStart(2, "0")

  return `${year}-${month}-${day}`
}