import { Check, X, Minus } from "lucide-react";
import { cn } from "@/lib/utils";
import { GuideSection } from "./GuideSection";

// 진성/비진성/불확실 분류 기준 안내 — 결과 해석 기준을 노출.
// ClauseTab의 "절 유형(Aspect) 안내"와 동일한 details 카드 템플릿(기본 접힘).
//
// 특정 축제/주제에 하드코딩하지 않고 "분석 대상" 중립 표현으로 작성한다. 기준
// 본문 예시도 특정 도메인(축제)에 치우치지 않도록 중립 표현으로 작성한다.

function Tier({
  icon: Icon,
  title,
  subtitle,
  accent,
  children,
}: {
  icon: typeof Check;
  title: string;
  subtitle: string;
  accent: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-xl border border-zinc-100 bg-zinc-50/50 p-3.5">
      <div className="flex items-center gap-2">
        <span
          className={cn(
            "inline-grid h-6 w-6 shrink-0 place-items-center rounded-full",
            accent,
          )}
        >
          <Icon className="h-3.5 w-3.5" />
        </span>
        <span className="text-sm font-bold text-zinc-800">{title}</span>
        <span className="text-xs text-zinc-400">{subtitle}</span>
      </div>
      <div className="mt-2.5 space-y-2 pl-8 text-xs leading-relaxed text-zinc-600">
        {children}
      </div>
    </div>
  );
}

// 분석 대상 강조 — 본문 중 대상 표기를 일관되게 처리.
function Subject() {
  return <b className="text-zinc-700">‘분석 대상’</b>;
}

export function GenuinenessCriteriaGuide() {
  return (
    <GuideSection title="진성 분류 기준 안내" meta="· 진성 / 비진성 / 불확실">
      <div className="space-y-3">
        {/* 진성 */}
        <Tier
          icon={Check}
          title="진성"
          subtitle="실제 방문·이용이 확인된 후기"
          accent="bg-emerald-50 text-emerald-600"
        >
          <p className="text-zinc-500">
            아래 두 조건을 <b className="text-zinc-700">모두 충족</b>하는 문서만
            진성으로 분류됩니다.
          </p>
          <div className="flex items-center gap-2">
            <p className="font-semibold text-zinc-700">1. 정확한 대상 :</p>
            <p>
              다른 대상이 아닌, 반드시 <Subject />에 대한 내용이어야
              합니다.
            </p>
          </div>
          <div>
            <div className="flex items-center gap-2">
              <p className="font-semibold text-zinc-700">
                2. 현장 경험 증거 (과거/완료형) :
              </p>
              <p>
                직접 경험한 사람만 알 수 있는 생생한 내용이나 감상이 포함되어야
                합니다.
              </p>
            </div>
            <ul className="mt-1.5 list-disc space-y-1 pl-4 text-zinc-500">
              <li>
                <b className="text-zinc-700">명시적 방문·이용</b>: “다녀왔어요”,
                “방문했어요”, “써봤어요” 등 직접 경험을 나타내는 표현
              </li>
              <li>
                <b className="text-zinc-700">현장 관찰</b>: 직접 경험한 사람만 알
                수 있는 구체적 정황(혼잡도, 당시 상황·날씨, 전년 대비 변화 등)
              </li>
              <li>
                <b className="text-zinc-700">직접 참여</b>: 체험·관람·이용에 대한
                묘사와 당시의 감정
              </li>
            </ul>
          </div>
        </Tier>

        {/* 비진성 */}
        <Tier
          icon={X}
          title="비진성"
          subtitle="직접 경험 후기가 아닌 문서"
          accent="bg-red-50 text-red-600"
        >
          <p className="text-zinc-500">
            다음 중 <b className="text-zinc-700">하나라도 해당</b>하면 분석
            대상에서 제외됩니다.
          </p>
          <ul className="list-disc space-y-1 pl-4 text-zinc-500">
            <li>
              <b className="text-zinc-700">단순 정보 및 홍보</b>: 일정 안내, 추천
              리스트, 보도자료, 공식 공지사항 등
            </li>
            <li>
              <b className="text-zinc-700">운영·모집 목적</b>: 서포터즈, 셀러,
              모집 등 운영 목적의 게시글
            </li>
            <li>
              <b className="text-zinc-700">미경험 (기대/예정)</b>: “가고 싶다”,
              “기대된다”, “다음에 이용해봐야지” 등 계획만 있는 상태의 글
            </li>
            <li>
              <b className="text-zinc-700">타 대상 후기</b>: 주된 내용이 다른
              대상이며, <Subject />은 단순 키워드로만 스쳐가듯
              언급된 글
            </li>
            <li>
              <b className="text-zinc-700">공식 어조</b>: 개인의 경험이 아닌,
              3인칭 시점이나 기관 공식 계정 톤으로 작성된 글
            </li>
          </ul>
        </Tier>

        {/* 불확실 */}
        <Tier
          icon={Minus}
          title="불확실"
          subtitle="판단이 불가능한 문서"
          accent="bg-zinc-100 text-zinc-500"
        >
          <p className="text-zinc-500">
            정보가 너무 부족하여 AI가 판별을 보류한 문서입니다.
          </p>
          <ul className="list-disc space-y-1 pl-4 text-zinc-500">
            <li>
              글의 길이가 너무 짧거나, 본문 없이 해시태그나 링크만 있는 경우
            </li>
            <li>
              작성자가 <Subject />을 직접 경험한 것인지, 아니면 다른
              대상을 경험한 것인지 문맥상 구분이 불가능한 경우
            </li>
          </ul>
        </Tier>

        {/* 참고 */}
        <div className="rounded-xl border border-violet-100 bg-violet-50/50 px-3.5 py-2.5 text-xs leading-relaxed text-violet-800">
          참고: AI는 위 기준을 바탕으로 분석하며, 직접 경험했다는 간접적인 정황이
          있더라도 그 대상이 <Subject />임이 명확하지 않다면 진성
          문서로 인정하지 않습니다.
        </div>
      </div>
    </GuideSection>
  );
}
