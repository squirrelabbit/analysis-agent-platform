import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { Dataset } from "../../types/dataset";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useNavigate } from "react-router-dom";
import { EmptyForm } from "@/components/common/EmptyForm";
import { Database } from "lucide-react";

export default function DatasetTable({ datasets }: { datasets: Dataset[] }) {
  const navigate = useNavigate();

  return datasets.length === 0 ? (
    <EmptyForm
      title="등록된 데이터셋이 없습니다"
      description="데이터셋을 먼저 등록한 뒤 파일을 업로드하세요"
      icon={<Database className="text-zinc-400" />}
    />
  ) : (
    <div className="overflow-hidden rounded-md border shadow">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>데이터셋명</TableHead>
            <TableHead>타입</TableHead>
            <TableHead>설명</TableHead>
            <TableHead>활성 데이터 유무</TableHead>
            <TableHead>등록일</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody className="bg-white">
          {datasets.map((d) => (
            <TableRow key={d.id}>
              <TableCell className="font-bold">{d.name}</TableCell>
              <TableCell>
                {d.dataType === "unstructured" ? (
                  <Badge className="bg-blue-50 text-blue-700">
                    비정형
                  </Badge>
                ) : (
                  <Badge className="bg-green-50 text-green-700">
                    정형
                  </Badge>
                )}
              </TableCell>
              <TableCell>{d.description}</TableCell>
              <TableCell>{d.activeDatasetVersionId ? "Y" : "N"}</TableCell>
              <TableCell>{d.createdAt.slice(0, 10)}</TableCell>
              <TableCell>
                <Button
                  size="xs"
                  variant="outline"
                  className="cursor-pointer hover:bg-indigo-50"
                  onClick={() => {
                    navigate(`${d.id}`);
                  }}
                >
                  데이터 관리
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
