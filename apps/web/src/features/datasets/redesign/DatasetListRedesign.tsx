import { useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  ChevronRight,
  Copy,
  Database,
  Layers,
  MoreVertical,
  Pencil,
  Plus,
  Trash2,
} from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { useDatasets } from "../hooks/dataset.query";
import { useDeleteDataset } from "../hooks/dataset.mutation";
import type { Dataset } from "../models/model";
import { useDatasetVersionStats } from "./useDatasetVersionStats";
import CreateDatasetDialogControlled from "./CreateDatasetDialogControlled";
import EditInfoDialogControlled from "./EditInfoDialogControlled";
import styles from "./DatasetListRedesign.module.css";

export default function DatasetListRedesign() {
  const navigate = useNavigate();
  const { projectId } = useProjectParams();
  const { data: project } = useProjectDetail(projectId);
  const { data: datasets = [] } = useDatasets();
  const [createOpen, setCreateOpen] = useState(false);

  return (
    <div className={styles.page}>
      <div className={styles.inner}>
        {/* breadcrumbs */}
        <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            { label: project?.name ?? "프로젝트" },
          ]}
        />

        {/* head */}
        <div className={styles.head}>
          <div className={styles.htext}>
            <h1>데이터셋</h1>
            <p>
              이 프로젝트에서 사용할 데이터셋을 등록·관리합니다. 데이터셋별로{" "}
              <u>프롬프트</u>와 업로드 데이터가 연결됩니다.
            </p>
          </div>
          <button className={styles.btnPrimary} onClick={() => setCreateOpen(true)}>
            <Plus />데이터셋 생성
          </button>
        </div>

        {/* cards grid */}
        <div className={styles.grid}>
          {datasets.map((d) => (
            <DatasetCard
              key={d.id}
              dataset={d}
              onOpen={() => navigate(d.id)}
            />
          ))}

          <button className={styles.addCard} onClick={() => setCreateOpen(true)}>
            <span className={styles.plus}>
              <Plus />
            </span>
            <span>데이터셋 생성</span>
          </button>
        </div>
      </div>

      <CreateDatasetDialogControlled
        open={createOpen}
        onOpenChange={setCreateOpen}
      />
    </div>
  );
}

function DatasetCard({
  dataset,
  onOpen,
}: {
  dataset: Dataset;
  onOpen: () => void;
}) {
  const { id, name, description, dataType } = dataset;
  const { data: stats, isPending } = useDatasetVersionStats(id);
  const { mutate: removeDataset } = useDeleteDataset();

  const [editOpen, setEditOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  const versionCount = stats?.versionCount ?? 0;
  const hasVersions = versionCount > 0;
  const hasActive = !!stats && stats.activeVersionNumber > 0;

  const kebab = (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button className={styles.kebab} onClick={(e) => e.stopPropagation()}>
          <MoreVertical />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
        <DropdownMenuItem onClick={() => setEditOpen(true)}>
          <Pencil />수정
        </DropdownMenuItem>
        <DropdownMenuItem onClick={onOpen}>
          <Plus />새 버전 추가
        </DropdownMenuItem>
        <DropdownMenuItem disabled>
          <Copy />복제
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          variant="destructive"
          className={styles.menuDanger}
          onClick={() => setDeleteOpen(true)}
        >
          <Trash2 />삭제
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );

  return (
    <article className={styles.card} onClick={onOpen} role="button" tabIndex={0}>
      <div className={styles.topRow}>
        <div className={styles.dsIcon}>
          <Database />
        </div>
        <div className={styles.nameWrap}>
          <span className={styles.dsName}>{name}</span>
          <span className={styles.tag}>
            {dataType === "structured" ? "정형" : "비정형"}
          </span>
          {hasVersions && (
            <span className={styles.verBadge}>
              <Layers />버전 {versionCount}
            </span>
          )}
        </div>
        {hasVersions && (
          <button
            className={styles.verManage}
            onClick={(e) => {
              e.stopPropagation();
              onOpen();
            }}
          >
            버전 관리
            <ChevronRight />
          </button>
        )}
        {kebab}
      </div>

      {description && <div className={styles.dsDesc}>{description}</div>}

      {isPending ? (
        <div className={styles.summaryMuted}>불러오는 중…</div>
      ) : hasVersions ? (
        <div className={styles.summary}>
          <span className={styles.lab}>활성 버전</span>
          {hasActive ? (
            <>
              <b>v{stats?.activeVersionNumber}</b>
              <span className={styles.sep}>·</span>
              <span className={styles.file}>{stats?.activeFileName}</span>
            </>
          ) : (
            <b>없음</b>
          )}
          <span className={styles.sep}>·</span>
          <span className={styles.lab}>최근 업로드</span>
          <b>{stats?.latestUpload}</b>
        </div>
      ) : (
        <div className={styles.emptyRow}>
          <span className={styles.emptyText}>아직 업로드된 버전이 없습니다.</span>
          <button
            className={styles.cta}
            onClick={(e) => {
              e.stopPropagation();
              onOpen();
            }}
          >
            <Plus />첫 버전 업로드
          </button>
        </div>
      )}

      {/* controlled dialogs (케밥에서 state로 연다 — dropdown 안에 dialog를 중첩하지 않음).
          다이얼로그는 포털로 body에 렌더되지만 React 이벤트는 트리를 따라 버블링되므로,
          내부 클릭(X·오버레이)이 카드 onClick(=버전 이동)으로 새지 않게 wrapper에서 차단. */}
      <div onClick={(e) => e.stopPropagation()}>
        <EditInfoDialogControlled
          dataset={dataset}
          open={editOpen}
          onOpenChange={setEditOpen}
        />
        <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
          <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>데이터셋 삭제</DialogTitle>
            <DialogDescription className="text-xs">
              정말 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.
            </DialogDescription>
            <div className="text-sm">데이터셋명: {name}</div>
          </DialogHeader>
          <DialogFooter className="flex gap-2">
            <DialogClose asChild>
              <Button variant="outline">취소</Button>
            </DialogClose>
            <DialogClose asChild>
              <Button variant="destructive" onClick={() => removeDataset(id)}>
                삭제
              </Button>
            </DialogClose>
          </DialogFooter>
        </DialogContent>
        </Dialog>
      </div>
    </article>
  );
}
