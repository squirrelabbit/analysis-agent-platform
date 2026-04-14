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

export default function ScenarioTab() {
  return (
    <div>
      <DialogForm btnText="+ 시나리오 등록" title="시나리오 등록">
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
                  <FieldLabel htmlFor="name-1">시나리오 이름</FieldLabel>
                  <Input
                    id="name-1"
                    name="name"
                    placeholder="시나리오 이름 입력"
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="name-1">형식</FieldLabel>
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
                    시나리오
                  </FieldLabel>
                  <Textarea
                  />
                </Field>
              </FieldGroup>
            </form>
          </TabsContent>
        </Tabs>
      </DialogForm>
    </div>
  );
}
