import { FileText, X } from "lucide-react"

export default function FilePreview({
  file,
  onRemove,
}: {
  file: File
  onRemove: () => void
}) {
  return (
    <div className="flex items-center gap-3 px-4 py-3 border border-indigo-200 bg-indigo-50 rounded-xl">
      <div className="w-8 h-8 rounded-lg bg-indigo-100 flex items-center justify-center shrink-0">
        <FileText className="w-4 h-4 text-indigo-500" />
      </div>

      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-zinc-800 truncate">
          {file.name}
        </p>
        <p className="text-[11px] text-zinc-400">
          {(file.size / 1024 / 1024).toFixed(2)} MB
        </p>
      </div>

      <button
        type="button"
        onClick={onRemove}
        className="w-6 h-6 flex items-center justify-center rounded-md text-zinc-400 hover:text-zinc-600 hover:bg-zinc-100"
      >
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
  )
}