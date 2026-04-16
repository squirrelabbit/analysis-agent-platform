import DialogForm from "@/components/common/DialogForm";
import FileUpload from "@/components/common/FileUpload";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";

export default function PromptTab() {
  return (
    <div>
      <div className="pb-2">
      {/* <div className="pb-2 place-self-end"> */}
        <DialogForm btnText="+ 프롬프트 등록" title="프롬프트 등록">
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
                    <FieldLabel htmlFor="name-1">프롬프트 이름</FieldLabel>
                    <Input
                      id="name-1"
                      name="name"
                      placeholder="프롬프트 이름 입력"
                    />
                  </Field>
                  <Field>
                    <FieldLabel htmlFor="checkout-7j9-optional-comments">
                      프롬프트
                    </FieldLabel>
                    <Textarea />
                  </Field>
                </FieldGroup>
              </form>
            </TabsContent>
          </Tabs>
        </DialogForm>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>프로젝트 프롬프트</CardTitle>
          <CardDescription>현재 v1 · 2025.01.14</CardDescription>
        </CardHeader>
        <Separator />
        <CardContent>
          <div>
            You are preparing raw VOC or issue text for deterministic downstream analysis.
            <br />
            - Keep the original meaning.<br />
            - Remove only obvious noise, duplicated punctuation, and boilerplate.<br />
            - Do not summarize beyond a short normalization.<br />
            - Do not invent facts.<br />
            - Choose disposition `keep`, `review`, or `drop`.<br />
            - Use `drop` only for empty, unreadable noise, or clear non-content rows.<br />
            - Use `review` when the text is partially readable but low quality or mixed.<br />

          </div>
        </CardContent>
      </Card>
      {/* <Textarea /> */}
    </div>
  )
}