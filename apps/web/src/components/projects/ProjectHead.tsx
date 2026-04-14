import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "../ui/input-group";
import { Search } from "lucide-react";
import DialogForm from "../common/DialogForm";
import { Field, FieldGroup, FieldLabel } from "../ui/field";
import { Input } from "../ui/input";

export default function ProjectHead() {
  return (
    <div>
      <div className="flex justify-between items-center pb-2">
        <div>
          <div>전체 프로젝트</div>
          <div className="text-xs">6개</div>
        </div>
        <DialogForm btnText="+ 새 프로젝트" title="프로젝트 등록">
          <form>
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="name-1">프로젝트 이름</FieldLabel>
                <Input
                  id="name-1"
                  name="name"
                  placeholder="프로젝트 이름 입력"
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="description-1">프로젝트 설명</FieldLabel>
                <Input
                  id="description-1"
                  name="description"
                  placeholder="프로젝트 설명 입력"
                />
              </Field>
            </FieldGroup>
          </form>
        </DialogForm>
      </div>
      <InputGroup>
        <InputGroupInput placeholder="Search..." />
        <InputGroupAddon>
          <Search />
        </InputGroupAddon>
      </InputGroup>
    </div>
  )
}