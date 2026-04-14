import DialogForm from "@/components/common/DialogForm";
import FileUpload from "@/components/common/FileUpload";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { MOCK_DATASETS, type Dataset } from "@/mock/datasetMockData";
import { FileText, Import, Trash2 } from "lucide-react";
import { useState } from "react";

export default function DatasetTab() {
  const [datasets, setDatasets] = useState<Dataset[]>(MOCK_DATASETS);

  function handleDelete(id: string) {
    setDatasets((prev) => prev.filter((ds) => ds.id !== id));
  }
  return (
    <div>
      <DialogForm btnText="+ 데이터셋 등록" title="데이터셋 등록">
        <Tabs defaultValue="upload">
          <TabsList className="w-full">
            <TabsTrigger value="upload">파일 업로드</TabsTrigger>
            <TabsTrigger value="input">직접 입력</TabsTrigger>
          </TabsList>
          <TabsContent value="upload">
            <FileUpload />
          </TabsContent>
          <TabsContent value="input">
            <form>
              <FieldGroup>
                <Field>
                  <FieldLabel htmlFor="name-1">데이터셋 이름</FieldLabel>
                  <Input
                    id="name-1"
                    name="name"
                    placeholder="데이터셋 이름 입력"
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="name-1">데이터 형식</FieldLabel>
                  <Select defaultValue="json">
                    <SelectTrigger id="checkout-exp-month-ts6">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectGroup>
                        <SelectItem value="json">json</SelectItem>
                        <SelectItem value="text">text</SelectItem>
                      </SelectGroup>
                    </SelectContent>
                  </Select>
                </Field>
                <Field>
                  <FieldLabel htmlFor="checkout-7j9-optional-comments">
                    데이터
                  </FieldLabel>
                  <Textarea
                  // id="checkout-7j9-optional-comments"
                  // placeholder="Add any additional comments"
                  // className="resize-none"
                  />
                </Field>
              </FieldGroup>
            </form>
          </TabsContent>
        </Tabs>
      </DialogForm>
      <div className="flex flex-col gap-2 pt-2">
        {datasets.length === 0 ? (
          <p className="text-xs text-zinc-400 text-center py-6">
            등록된 데이터셋이 없습니다
          </p>
        ) : (
          datasets.map((ds) => (
            <Item
              variant={"muted"}
              className="group border border-zinc-100 rounded-lg bg-zinc-50 hover:border-violet-200 hover:bg-violet-50"
            >
              <ItemMedia className="w-7 h-7 rounded-md bg-zinc-100 flex items-center justify-center shrink-0 group-hover:bg-violet-100">
                <FileText className="w-3 h-3 text-zinc-500 group-hover:text-violet-600" />
              </ItemMedia>
              <ItemContent>
                <ItemTitle className="text-xs">{ds.name}</ItemTitle>
                <ItemDescription className="text-[10px]">
                  {ds.type.toUpperCase()} · {ds.size} · {ds.updatedAt}
                </ItemDescription>
              </ItemContent>
              <ItemActions className="hover:text-gray-500 hover:bg-gray-50 p-1.5 rounded-lg">
                <span className="text-[10px] px-2 py-0.5 rounded-full bg-violet-100 text-violet-700 border border-violet-200">
                  v{ds.version}
                </span>
                {/* <Badge variant='outline'  className="bg-violet-100 text-violet-600 text-sm">v{ds.version}</Badge> */}
              </ItemActions>
              <ItemActions className="hover:text-gray-500 hover:bg-gray-50 p-1.5 rounded-lg">
                <Import className="size-4" />
              </ItemActions>
              <ItemActions onClick={() =>handleDelete(ds.id)} className="hover:text-red-500 hover:bg-red-50 p-1.5 rounded-lg">
                <Trash2 className="size-4" />
              </ItemActions>
            </Item>
          ))
        )}
      </div>
    </div>
    // <Tabs defaultValue="registry">
    //     <TabsList >
    //       <TabsTrigger value="registry">데이터 등록</TabsTrigger>
    //       <TabsTrigger value="history">버전 이력</TabsTrigger>
    //     </TabsList>
    //     <TabsContent value="registry">
    //       {/* <DatasetUploadFile /> */}
    //     </TabsContent>
    //     <TabsContent value="history">
    //       {/* <DatasetVersion /> */}
    //     </TabsContent>
    //   </Tabs>
  );
}
