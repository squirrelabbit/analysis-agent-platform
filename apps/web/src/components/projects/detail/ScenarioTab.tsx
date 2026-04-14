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
import { MOCK_SCENARIOS } from "@/mock/scenarioMockData";
import { SquarePen } from "lucide-react";

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
                  <Textarea />
                </Field>
              </FieldGroup>
            </form>
          </TabsContent>
        </Tabs>
      </DialogForm>
      <div className="flex flex-col gap-2 pt-2">
        {MOCK_SCENARIOS.length === 0 ? (
          <p className="text-xs text-zinc-400 text-center py-6">
            등록된 데이터셋이 없습니다
          </p>
        ) : (
          MOCK_SCENARIOS.map((ds) => (
            <Item
              variant={"muted"}
              className="group border border-zinc-100 rounded-lg bg-zinc-50 hover:border-violet-200 hover:bg-violet-50"
            >
              <ItemMedia className="w-7 h-7 rounded-md bg-zinc-100 flex items-center justify-center shrink-0 group-hover:bg-violet-100">
                {ds.scenario_id}
              </ItemMedia>
              <ItemContent>
                <ItemTitle className="text-xs">
                  [{ds.query_type}] {ds.user_query}
                </ItemTitle>
                <ItemDescription className="text-[10px]">
                  {ds.analysis_scope} · {ds.interpretation} ·{" "}
                  {ds.created_at.slice(0, 10)}
                </ItemDescription>
              </ItemContent>
              <ItemActions className="hover:text-gray-500 hover:bg-gray-50 p-1.5 rounded-lg">
                <span className="text-[10px] px-2 py-0.5 rounded-full bg-violet-100 text-violet-700 border border-violet-200">
                  {ds.planning_mode}
                </span>
              </ItemActions>
              <ItemActions className="hover:text-blue-600 hover:bg-blue-50 p-1.5 rounded-lg">
                <SquarePen className="size-4" />
              </ItemActions>
            </Item>
          ))
        )}
      </div>
    </div>
  );
}
