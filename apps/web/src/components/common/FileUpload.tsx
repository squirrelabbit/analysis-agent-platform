import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "../ui/empty";
import { Input } from "../ui/input";
import { CloudUpload, Trash, Upload } from "lucide-react";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "../ui/item";
import { Button } from "../ui/button";

export default function FileUpload() {
  const [files, setFiles] = useState<File[]>([]);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const mapped = acceptedFiles.map((file) =>
      Object.assign(file, {
        preview: URL.createObjectURL(file),
      }),
    );
    setFiles(mapped);
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop: onDrop,
  });

  const handleRemove = (name: string) => {
    setFiles((prev) => prev.filter((file) => file.name !== name));
  };

  return (
    <div>
      {files.length === 0 ? (
        <Empty
          {...getRootProps()}
          className={`border border-dashed ${isDragActive ? "border-blue-400 bg-blue-50" : "border-gray-300 hover:bg-gray-50"}`}
        >
          <Input {...getInputProps()} />
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <CloudUpload />
            </EmptyMedia>
            <EmptyTitle>파일 업로드</EmptyTitle>
            <EmptyDescription>
              {isDragActive
                ? "파일을 여기에 놓으세요"
                : "클릭 또는 드래그로 업로드 (.csv, .json)"}
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className="flex flex-col gap-1.5">
          {files.map((file) => (
            <Item variant="outline">
              <ItemContent className="flex justify-between">
                <ItemTitle>{file.name}</ItemTitle>
                <ItemDescription>
                  {file.lastModified} · {file.size}
                </ItemDescription>
              </ItemContent>
              <ItemContent>
                <ItemActions>
                  <Button size="sm" variant="secondary">
                    <Upload />
                    업로드
                  </Button>
                  <Button
                    size="sm"
                    variant="destructive"
                    onClick={() => handleRemove(file.name)}
                  >
                    <Trash />
                    삭제
                  </Button>
                </ItemActions>
              </ItemContent>
            </Item>
          ))}
        </div>
      )}
    </div>
  );
}
