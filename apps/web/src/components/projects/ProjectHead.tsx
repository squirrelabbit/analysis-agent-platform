import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "../ui/input-group";
import { Button } from "../ui/button";
import { Search } from "lucide-react";

export default function ProjectHead() {
  return (
    <div>
      <div className="flex justify-between items-center pb-2">
        <div>
          <div>전체 프로젝트</div>
          <div className="text-xs">6개</div>
        </div>
        <Button>+ 새 프로젝트</Button>
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