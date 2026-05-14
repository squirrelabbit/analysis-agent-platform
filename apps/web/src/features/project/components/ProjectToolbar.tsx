import { Field, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { LayoutGrid, List, Search } from "lucide-react";

interface ProjectToolbarProps {
  search: string;
  onSearchChange: (v: string) => void;
  view: "list" | "grid";
  onViewChange: (v: "list" | "grid") => void;
  pageSize: number;
  onPageSizeChange: (page: number) => void;
  totalCount: number;
}

export default function ProjectToolbar({
  search,
  onSearchChange,
  view,
  onViewChange,
  pageSize,
  onPageSizeChange,
  totalCount,
}: ProjectToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-3 py-2">
      {/* 검색 입력 */}
      <div className="flex-1 relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
        <Input
          type="text"
          placeholder="프로젝트 검색..."
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          className="pl-10 pr-4 py-2 rounded-lg border border-slate-200 bg-white  transition-all duration-300"
        />
      </div>
      <div className="flex flex-1 gap-3 items-center place-content-end">
        {/* 뷰 전환 탭 */}
        <Tabs
          value={view}
          onValueChange={(v) => onViewChange(v as "list" | "grid")}
        >
          <TabsList className="transition-all duration-300 ">
            <TabsTrigger value="grid">
              <LayoutGrid /> 그리드
            </TabsTrigger>
            <TabsTrigger value="row">
              <List /> 리스트
            </TabsTrigger>
          </TabsList>
        </Tabs>

        {/* 페이지당 항목 개수 선택 */}
        <Field orientation="horizontal" className="w-fit">
          <FieldLabel htmlFor="select-rows-per-page">페이지당 개수</FieldLabel>
          <Select
            value={pageSize.toString()}
            onValueChange={(v) => onPageSizeChange(Number(v))}
          >
            <SelectTrigger className="w-20 bg-white" id="select-rows-per-page">
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="start">
              <SelectGroup>
                <SelectItem value="10">10</SelectItem>
                <SelectItem value="25">25</SelectItem>
                <SelectItem value="50">50</SelectItem>
                <SelectItem value="100">100</SelectItem>
              </SelectGroup>
            </SelectContent>
          </Select>
        </Field>

        {/* 총 개수 표시 */}
        <div className="text-xs text-violet-500">전체 {totalCount}개</div>
      </div>
    </div>
  );
}
