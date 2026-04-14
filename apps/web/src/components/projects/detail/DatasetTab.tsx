import DialogForm from "@/components/common/DialogForm";
import FileUpload from "@/components/common/FileUpload";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
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

export default function DatasetTab() {
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
