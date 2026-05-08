import { useState } from "react"
import { Upload } from "lucide-react"
import { cn } from "@/lib/utils"

export default function FileDropzone({
  onSelect,
  accept,
}: {
  onSelect: (file: File) => void
  accept?: string
}) {
  const [isDragging, setIsDragging] = useState(false)

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)

    const file = e.dataTransfer.files?.[0]
    if (file) onSelect(file)
  }

  return (
    <label
      className={cn(
        "flex flex-col items-center gap-2 py-5 border-[1.5px] border-dashed rounded-xl cursor-pointer transition-colors",
        isDragging
          ? "border-indigo-400 bg-indigo-50"
          : "border-zinc-200 hover:border-indigo-300 hover:bg-zinc-50"
      )}
      onDragOver={(e) => {
        e.preventDefault()
        setIsDragging(true)
      }}
      onDragLeave={() => setIsDragging(false)}
      onDrop={handleDrop}
    >
      <div
        className={cn(
          "w-9 h-9 rounded-xl flex items-center justify-center",
          isDragging ? "bg-indigo-100" : "bg-zinc-100"
        )}
      >
        <Upload
          className={cn(
            "w-4 h-4",
            isDragging ? "text-indigo-500" : "text-zinc-400"
          )}
        />
      </div>

      <div className="text-center">
        <p className="text-sm text-zinc-600">클릭하거나 드래그하세요</p>
        <p className="text-[11px] text-zinc-400 mt-0.5">
          {accept || "파일 업로드"}
        </p>
      </div>

      <input
        type="file"
        className="hidden"
        accept={accept}
        onChange={(e) => {
          const f = e.target.files?.[0]
          if (f) onSelect(f)
        }}
      />
    </label>
  )
}