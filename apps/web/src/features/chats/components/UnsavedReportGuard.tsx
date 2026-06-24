import { useEffect } from "react";
import { useBlocker } from "react-router-dom";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

// 미저장 보고서 편집 가드.
// 채팅 보고서 패널에 담긴(아직 보고서로 저장하지 않은) 결과가 있을 때, 다른 페이지로
// 이동(앱 내 라우팅)하거나 탭을 닫으면 경고한다. active=false면 가드 비활성(보고서
// 생성으로 인한 의도된 이동 등에서 끈다).
export default function UnsavedReportGuard({ active }: { active: boolean }) {
  // 앱 내 페이지 이동 차단 — blocked 상태가 되면 확인창을 띄운다.
  const blocker = useBlocker(active);

  // 브라우저 탭 닫기/새로고침 경고(native).
  useEffect(() => {
    if (!active) return;
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [active]);

  const blocked = blocker.state === "blocked";

  return (
    <Dialog
      open={blocked}
      onOpenChange={(o) => {
        // ESC·바깥 클릭으로 닫으면 이동 취소(머무르기).
        if (!o) blocker.reset?.();
      }}
    >
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>저장하지 않은 보고서 편집</DialogTitle>
        </DialogHeader>
        <p className="text-sm text-zinc-600">
          보고서에 추가한 결과가 아직 보고서로 저장되지 않았습니다. 이 페이지를
          떠나면 편집 내용이 사라집니다. 나가시겠어요?
        </p>
        <DialogFooter className="flex gap-2">
          <Button variant="outline" onClick={() => blocker.reset?.()}>
            취소
          </Button>
          <Button variant="destructive" onClick={() => blocker.proceed?.()}>
            나가기
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
