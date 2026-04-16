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

export default function FileUpload({ title }: { title?: string }) {
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
          className={`group border border-dashed hover:bg-violet-50 hover:border-violet-600 ${isDragActive ? "hover:bg-violet-50 hover:border-violet-600" : "border-gray-300 "}`}
        >
          <Input {...getInputProps()} />
          <EmptyHeader>
            <EmptyMedia
              variant="icon"
              className="group-hover:bg-violet-100 transition-colors"
            >
              <CloudUpload className=" w-5 h-5 text-zinc-400" />
            </EmptyMedia>
            <EmptyTitle>{title || "파일 업로드"}</EmptyTitle>
            <EmptyDescription className="text-xs">
              {isDragActive
                ? "파일을 여기에 놓으세요"
                : "클릭 또는 드래그로 파일을 업로드하세요"}
            </EmptyDescription>
          </EmptyHeader>
          {/* <EmptyContent>
            <Button
              size="sm"
              variant="outline"
              // onClick={() => onUpload(dataset)}
              className="h-7 text-xs border-violet-200  hover:bg-violet-50"
            >
              <Upload className="w-3 h-3 mr-1.5" />
              파일 업로드
            </Button>
          </EmptyContent> */}
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
