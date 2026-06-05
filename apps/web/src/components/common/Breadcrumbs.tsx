import { Fragment } from "react";
import { ChevronRight } from "lucide-react";
import { useNavigate } from "react-router-dom";
import styles from "./Breadcrumbs.module.css";

export interface Crumb {
  label: string;
  /** 지정 시 클릭하면 이동(SPA). 마지막 항목은 현재 페이지로 항상 비링크. */
  to?: string;
}

/*
 * 데이터셋 계열 페이지(데이터셋 목록 / 버전 목록 / 데이터 처리 상세) 공용 breadcrumb.
 * 체계: 프로젝트 > {프로젝트명} > {데이터셋명} > [현재]. 마지막 항목은 bold 현재 표시.
 */
export default function Breadcrumbs({ items }: { items: Crumb[] }) {
  const navigate = useNavigate();
  return (
    <div className={styles.crumbs}>
      {items.map((item, index) => {
        const isLast = index === items.length - 1;
        return (
          <Fragment key={index}>
            {index > 0 && <ChevronRight className={styles.sep} />}
            {isLast || !item.to ? (
              <b>{item.label}</b>
            ) : (
              <a onClick={() => navigate(item.to as string)}>{item.label}</a>
            )}
          </Fragment>
        );
      })}
    </div>
  );
}
