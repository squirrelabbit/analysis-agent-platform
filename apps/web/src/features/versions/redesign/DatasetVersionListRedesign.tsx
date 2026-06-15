import { useState, type MouseEvent } from "react";
import { useNavigate } from "react-router-dom";
import {
  Calendar,
  CheckCircle2,
  Download,
  FileText,
  GitCompare,
  Layers,
  MoreVertical,
  Plus,
  Rows3,
  Zap,
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
import { formatFileSize } from "@/shared/utils/format";
import { useDownloadFile } from "@/shared/apis/common.mutation";
import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { useDataset } from "@/features/datasets/hooks/dataset.query";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import CreateVersionForm from "@/features/versions/components/forms/CreateVersionForm";
import { buildLabel } from "@/shared/constants/buildLabels";
import {
  useActiveVersion,
  useCreateVersion,
  useDeleteVersion,
} from "../hooks/version.mutation";
import { useVersionsWithNumber } from "./useVersionsWithNumber";
import type { NumberedVersion } from "./useVersionsWithNumber";
import styles from "./DatasetVersionListRedesign.module.css";

// "2026-05-28T17:41:00Z" → "2026-05-28 17:41"
function fmtDateTime(iso: string): string {
  if (!iso) return "—";
  return iso.slice(0, 16).replace("T", " ");
}

const PIPELINE: { key: keyof Pick<NumberedVersion, "cleanStatus" | "docGenuinenessStatus" | "clauseLabelStatus">; label: string }[] = [
  { key: "cleanStatus", label: buildLabel("clean") },
  { key: "docGenuinenessStatus", label: buildLabel("doc_genuineness") },
  { key: "clauseLabelStatus", label: buildLabel("clause_label") },
];

interface Props {
  /** "새 버전 업로드" 버튼 핸들러. 미지정 시 버튼은 보이되 동작 없음(업로드 모달은 별도 작업). */
  onNewVersion?: () => void;
}

export default function DatasetVersionListRedesign({ onNewVersion }: Props) {
  const navigate = useNavigate();
  const { projectId, datasetId } = useDatasetParams();
  const { data: project } = useProjectDetail(projectId);
  const { data: dataset } = useDataset();
  const { data: versions = [], isLoading } = useVersionsWithNumber();

  const { mutate: activate } = useActiveVersion();
  const { mutate: remove } = useDeleteVersion();
  const { mutate: download } = useDownloadFile();
  const { mutateAsync: createVersion } = useCreateVersion();
  const [uploadOpen, setUploadOpen] = useState(false);

  if (isLoading) return null;

  const activeVersion = versions.find((v) => v.isActive);
  const latest = versions[0]; // 이미 versionNumber 내림차순 정렬

  return (
    <div className={styles.page}>
      <div className={styles.inner}>
        {/* breadcrumbs */}
        <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            {
              label: project?.name ?? "프로젝트",
              to: `/projects/${projectId}/datasets`,
            },
            { label: dataset?.name ?? "데이터셋" },
          ]}
        />

        {/* head */}
        <div className={styles.head}>
          <div className={styles.htext}>
            <h1>{dataset?.name ?? "데이터셋 버전"}</h1>
            {dataset?.description ? (
              <div className={styles.sub}>{dataset.description}</div>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              className="h-9 gap-1.5"
              onClick={() =>
                navigate(
                  `/projects/${projectId}/datasets/${datasetId}/doc-genuineness-compare`,
                )
              }
            >
              <GitCompare className="h-4 w-4" />
              모델 비교
            </Button>
            <button
              className={styles.btnPrimary}
              onClick={() => (onNewVersion ? onNewVersion() : setUploadOpen(true))}
            >
              <Plus />새 버전 업로드
            </button>
          </div>
        </div>

        {/* summary bar */}
        <div className={styles.summary}>
          <span className={styles.s}>
            <Layers />버전 <b>{versions.length}개</b>
          </span>
          <span className={styles.divider} />
          <span className={styles.s}>
            <Zap />활성 버전{" "}
            <b>{activeVersion ? `v${activeVersion.versionNumber}` : "없음"}</b>
          </span>
          <span className={styles.divider} />
          <span className={styles.s}>
            <Calendar />최근 업로드{" "}
            <b>{latest ? fmtDateTime(latest.createdAt).split(" ")[0] : "—"}</b>
          </span>
        </div>

        {/* version list */}
        <div className={styles.vlist}>
          {versions.map((v) => (
            <VersionRow
              key={v.id}
              v={v}
              onDetail={() => navigate(v.id)}
              onDownload={() => download({ versionId: v.id, type: "source" })}
              onActivate={() => activate(v.id)}
              onDelete={() => remove(v.id)}
            />
          ))}

          {/* 하단 새 버전 업로드 추가 영역 (데이터셋 페이지와 동일) */}
          <button className={styles.addCard} onClick={() => setUploadOpen(true)}>
            <span className={styles.plus}>
              <Plus />
            </span>
            <span>새 버전 업로드</span>
          </button>
        </div>
      </div>

      {/* 새 버전 업로드 (controlled). 기존 CreateVersionForm + useCreateVersion 재사용 */}
      <Dialog open={uploadOpen} onOpenChange={setUploadOpen}>
        <DialogContent className="sm:max-w-md flex flex-col max-h-[80vh]">
          <DialogHeader className="shrink-0">
            <DialogTitle>새 버전 업로드</DialogTitle>
          </DialogHeader>
          <div className="flex-1 overflow-y-auto">
            <CreateVersionForm
              formId="version-upload-form"
              type={dataset?.dataType ?? "unstructured"}
              onSubmit={async (data) => {
                await createVersion(data);
              }}
              onSuccess={() => setUploadOpen(false)}
            />
          </div>
          <DialogFooter className="flex gap-2">
            <Button variant="outline" onClick={() => setUploadOpen(false)}>
              취소
            </Button>
            <Button type="submit" form="version-upload-form">
              저장
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

interface RowProps {
  v: NumberedVersion;
  onDetail: () => void;
  onDownload: () => void;
  onActivate: () => void;
  onDelete: () => void;
}

function VersionRow({ v, onDetail, onDownload, onActivate, onDelete }: RowProps) {
  const [deleteOpen, setDeleteOpen] = useState(false);

  // 카드 전체 클릭으로 상세 이동. 내부 버튼은 카드 클릭과 충돌하지 않게 버블링 차단.
  const stop = (fn: () => void) => (e: MouseEvent) => {
    e.stopPropagation();
    fn();
  };

  return (
    <div
      className={`${styles.vrow} ${v.isActive ? styles.active : ""}`}
      onClick={onDetail}
      role="button"
      tabIndex={0}
    >
      <div className={styles.vId}>
        <div className={styles.vTag}>v{v.versionNumber}</div>
        {v.isActive ? (
          <span className={`${styles.vState} ${styles.on}`}>
            <CheckCircle2 />활성 버전
          </span>
        ) : (
          <span className={`${styles.vState} ${styles.off}`}>비활성</span>
        )}
      </div>

      <div className={styles.vBody}>
        <div className={styles.vFile}>{v.originalFilename}</div>
        <div className={styles.vMeta}>
          <span className={styles.mi}>
            <Calendar />
            {fmtDateTime(v.createdAt)}
          </span>
          <span className={styles.sep}>·</span>
          <span className={styles.mi}>
            <Rows3 />
            {v.rowCount.toLocaleString()}건
          </span>
          <span className={styles.sep}>·</span>
          <span className={styles.mi}>
            <FileText />
            {formatFileSize(v.byteSize)}
          </span>
        </div>
        <div className={styles.chips}>
          {PIPELINE.map(({ key, label }) => {
            const done = v[key] === "ready";
            return (
              <span
                key={key}
                className={`${styles.chip} ${done ? styles.done : styles.pend}`}
              >
                <span className={styles.dot} />
                {label} {done ? "완료" : "대기"}
              </span>
            );
          })}
        </div>
      </div>

      <div className={styles.vActions}>
        <div className={styles.actRow}>
          <button className={styles.btn} onClick={stop(onDetail)}>
            <FileText />상세 보기
          </button>
          <button className={styles.btn} onClick={stop(onDownload)}>
            <Download />다운로드
          </button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button
                className={`${styles.btn} ${styles.icon}`}
                onClick={(e) => e.stopPropagation()}
              >
                <MoreVertical />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
              <DropdownMenuItem disabled>이름 변경</DropdownMenuItem>
              <DropdownMenuItem disabled>메타데이터</DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem
                variant="destructive"
                onClick={() => setDeleteOpen(true)}
                className={styles.menuDanger}
              >
                삭제
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
        {!v.isActive && (
          <button className={styles.btnActivate} onClick={stop(onActivate)}>
            <Zap />이 버전 활성화
          </button>
        )}
      </div>

      {/* 삭제 확인 (행 onClick과 충돌하지 않게 wrapper에서 버블링 차단) */}
      <div onClick={(e) => e.stopPropagation()}>
        <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
          <DialogContent className="sm:max-w-sm">
            <DialogHeader>
              <DialogTitle>버전 삭제</DialogTitle>
              <DialogDescription className="text-xs">
                v{v.versionNumber} ({v.originalFilename})을(를) 삭제합니다. 이
                작업은 되돌릴 수 없습니다.
                {v.isActive && " 활성 버전이라 삭제 후 활성 버전이 없어집니다."}
              </DialogDescription>
            </DialogHeader>
            <DialogFooter className="flex gap-2">
              <DialogClose asChild>
                <Button variant="outline">취소</Button>
              </DialogClose>
              <DialogClose asChild>
                <Button variant="destructive" onClick={onDelete}>
                  삭제
                </Button>
              </DialogClose>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  );
}
