import type { MouseEvent } from "react";
import { useNavigate } from "react-router-dom";
import {
  Calendar,
  CheckCircle2,
  ChevronRight,
  Download,
  FileText,
  Info,
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
import { formatFileSize } from "@/shared/utils/format";
import { useDownloadFile } from "@/shared/apis/common.mutation";
import { useDatasetParams } from "@/shared/hooks/useRouteParams";
import { useDataset } from "@/features/datasets/hooks/dataset.query";
import { useActiveVersion, useDeleteVersion } from "../hooks/version.mutation";
import { useVersionsWithNumber } from "./useVersionsWithNumber";
import type { NumberedVersion } from "./useVersionsWithNumber";
import styles from "./DatasetVersionListRedesign.module.css";

// "2026-05-28T17:41:00Z" → "2026-05-28 17:41"
function fmtDateTime(iso: string): string {
  if (!iso) return "—";
  return iso.slice(0, 16).replace("T", " ");
}

const PIPELINE: { key: keyof Pick<NumberedVersion, "cleanStatus" | "docGenuinenessStatus" | "clauseLabelStatus">; label: string }[] = [
  { key: "cleanStatus", label: "정제" },
  { key: "docGenuinenessStatus", label: "진성 분류" },
  { key: "clauseLabelStatus", label: "절 라벨링" },
];

interface Props {
  /** "새 버전 업로드" 버튼 핸들러. 미지정 시 버튼은 보이되 동작 없음(업로드 모달은 별도 작업). */
  onNewVersion?: () => void;
}

export default function DatasetVersionListRedesign({ onNewVersion }: Props) {
  const navigate = useNavigate();
  const { projectId } = useDatasetParams();
  const { data: dataset } = useDataset();
  const { data: versions = [], isLoading } = useVersionsWithNumber();

  const { mutate: activate } = useActiveVersion();
  const { mutate: remove } = useDeleteVersion();
  const { mutate: download } = useDownloadFile();

  if (isLoading) return null;

  const activeVersion = versions.find((v) => v.isActive);
  const latest = versions[0]; // 이미 versionNumber 내림차순 정렬

  return (
    <div className={styles.page}>
      <div className={styles.inner}>
        {/* breadcrumbs */}
        <div className={styles.crumbs}>
          <a onClick={() => navigate("/projects")}>프로젝트</a>
          <ChevronRight />
          <a onClick={() => navigate(`/projects/${projectId}/datasets`)}>
            데이터셋
          </a>
          <ChevronRight />
          <b>{dataset?.name ?? "데이터셋 버전"}</b>
        </div>

        {/* head */}
        <div className={styles.head}>
          <div className={styles.htext}>
            <h1>{dataset?.name ?? "데이터셋 버전"}</h1>
            {dataset?.description ? (
              <div className={styles.sub}>{dataset.description}</div>
            ) : null}
          </div>
          <button className={styles.btnPrimary} onClick={onNewVersion}>
            <Plus />새 버전 업로드
          </button>
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
        {versions.length === 0 ? (
          <div className={styles.empty}>아직 업로드된 버전이 없습니다.</div>
        ) : (
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
          </div>
        )}

        <div className={styles.footNote}>
          <Info />
          버전은 삭제되지 않으며, 필요할 때 이전 버전을 활성화할 수 있습니다.
        </div>
      </div>
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
              {v.isActive ? (
                <DropdownMenuItem disabled className={styles.menuDisabled}>
                  삭제 불가 (활성 버전)
                </DropdownMenuItem>
              ) : (
                <DropdownMenuItem
                  variant="destructive"
                  onClick={stop(onDelete)}
                  className={styles.menuDanger}
                >
                  삭제
                </DropdownMenuItem>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
        {!v.isActive && (
          <button className={styles.btnActivate} onClick={stop(onActivate)}>
            <Zap />이 버전 활성화
          </button>
        )}
      </div>
    </div>
  );
}
