import { useEffect, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Field, FieldLabel, FieldDescription } from "@/components/ui/field";
import { useSetKeywordDictionaryRule } from "../hooks/build.query";

export type RefineMode = "exclude" | "synonym";

// 키워드 정제 모달 (silverone 2026-06-25). 제외=block, 대표어 지정=synonym.
// 원본 데이터는 건드리지 않고 dataset 정제 규칙만 추가한다(저장 즉시 결과 반영).
export default function KeywordRefineDialog({
  open,
  onOpenChange,
  mode,
  sourceTerm,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: RefineMode;
  sourceTerm: string;
  onSaved?: (message: string) => void;
}) {
  const [target, setTarget] = useState("");
  const [reason, setReason] = useState("");
  const [error, setError] = useState("");
  const { mutateAsync, isPending } = useSetKeywordDictionaryRule();

  // 열릴 때마다 초기화 (대상 키워드 바뀜).
  useEffect(() => {
    if (open) {
      setTarget("");
      setReason("");
      setError("");
    }
  }, [open, sourceTerm, mode]);

  const isSynonym = mode === "synonym";
  const canSave = !isPending && (!isSynonym || target.trim().length > 0);

  const submit = async () => {
    setError("");
    try {
      await mutateAsync({
        rule_type: isSynonym ? "synonym" : "block",
        source_term: sourceTerm,
        target_term: isSynonym ? target.trim() : undefined,
        reason: reason.trim() || undefined,
      });
      onSaved?.(
        isSynonym
          ? `"${sourceTerm}"이(가) "${target.trim()}"(으)로 병합되었습니다.`
          : `"${sourceTerm}"이(가) 키워드 결과에서 제외되었습니다.`,
      );
      onOpenChange(false);
    } catch (e) {
      const detail =
        (e as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail ?? "저장에 실패했습니다.";
      setError(detail);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{isSynonym ? "대표어 지정" : "키워드 제외"}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4 py-1">
          <Field>
            <FieldLabel className="text-xs">대상 키워드</FieldLabel>
            <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium">
              {sourceTerm}
            </div>
          </Field>

          {isSynonym ? (
            <Field>
              <FieldLabel className="text-xs">대표어</FieldLabel>
              <Input
                value={target}
                onChange={(e) => setTarget(e.target.value)}
                placeholder="예) 맥주"
              />
              <FieldDescription className="text-xs">
                {target.trim()
                  ? `${sourceTerm} → ${target.trim()} (으)로 병합됩니다.`
                  : "이 키워드를 합칠 대표어를 입력하세요."}
              </FieldDescription>
            </Field>
          ) : (
            <p className="text-xs text-slate-500">
              이 데이터셋의 키워드 결과에서 제외됩니다. 원본 데이터는 삭제되지
              않으며 [정제 규칙] 탭에서 해제할 수 있습니다.
            </p>
          )}

          <Field>
            <FieldLabel className="text-xs">사유 (선택)</FieldLabel>
            <Input
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder={isSynonym ? "같은 의미의 키워드 병합" : "분석과 무관한 일반어"}
            />
          </Field>

          {error && <p className="text-xs text-red-500">{error}</p>}
        </div>
        <DialogFooter className="flex gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            취소
          </Button>
          <Button onClick={submit} disabled={!canSave}>
            {isSynonym ? "병합" : "제외"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
