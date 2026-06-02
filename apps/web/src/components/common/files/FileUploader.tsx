import FilePreview from "./FilePreview"
import FileDropzone from "./FileDropzone"

type Props = {
  value?: File
  onChange: (file?: File) => void
  accept?: string
  maxSize?: number
}

export default function FileUploader({
  value,
  onChange,
  accept = ".csv,.json,.jsonl,.xlsx",
  maxSize = 100 * 1024 * 1024,
}: Props) {
  const handleFile = (file: File) => {
    if (file.size > maxSize) {
      alert("파일 크기 초과")
      return
    }

    onChange(file)
  }

  if (value) {
    return (
      <FilePreview
        file={value}
        onRemove={() => onChange(undefined)}
      />
    )
  }

  return (
    <FileDropzone
      onSelect={handleFile}
      accept={accept}
    />
  )
}