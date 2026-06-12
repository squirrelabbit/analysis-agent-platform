import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { apiClient } from "@/api/client";

// 로그인 화면 (디자인 「로그인.html」, ADR-025). 승인된 회사 Google 계정으로 로그인.
// 백엔드 /api/auth/google/start로 이동 → callback → 세션 쿠키 → 프론트 복귀.
// 커스텀 CSS(oklch/gradient/pseudo-element)는 lp- 프리픽스 scoped <style>로 픽셀 재현.

const ERROR_MESSAGES: Record<string, string> = {
  not_allowed: "허용되지 않은 계정입니다. 관리자에게 초대를 요청하세요.",
  wrong_account: "회사 Google 계정으로 로그인해 주세요.",
  session: "세션이 만료되었습니다. 다시 로그인해 주세요.",
};

const GOOGLE_START_URL = "/api/auth/google/start";

export default function LoginPage() {
  const [params] = useSearchParams();
  const [loading, setLoading] = useState(false);
  const errorCode = params.get("error") ?? "";
  const errorMessage = ERROR_MESSAGES[errorCode];

  // 이미 로그인 상태면 프로젝트로. (실패=미로그인은 무시하고 로그인 화면 유지)
  useEffect(() => {
    let cancelled = false;
    apiClient
      .get("/auth/me")
      .then(() => {
        if (!cancelled) window.location.href = "/projects";
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  const handleGoogle = () => {
    setLoading(true);
    window.location.href = GOOGLE_START_URL;
  };

  return (
    <div className="lp-root">
      <style>{LOGIN_CSS}</style>
      <div className="lp-wrap">
        <div className="lp-logo">
          <span className="lp-mark" />
          <span className="lp-wm">
            <span className="lp-w">WISE</span>
            <span className="lp-n">nut</span>
          </span>
        </div>

        <main className="lp-panel" role="main">
          <div className="lp-lead">
            <h1>분석시스템에 로그인</h1>
            <p>
              승인된 회사 Google 계정으로
              <br />
              프로젝트·데이터셋·분석 결과에 접근하세요.
            </p>
          </div>

          {errorMessage && (
            <div className="lp-alert" role="alert">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="9.5" />
                <line x1="12" y1="7.5" x2="12" y2="13" />
                <line x1="12" y1="16.5" x2="12" y2="16.5" />
              </svg>
              <span>{errorMessage}</span>
            </div>
          )}

          <button
            className={`lp-gbtn${loading ? " is-loading" : ""}`}
            type="button"
            onClick={handleGoogle}
            disabled={loading}
          >
            <svg className="lp-gicon" viewBox="0 0 24 24" aria-hidden="true">
              <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.27-4.74 3.27-8.1z" />
              <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84A11 11 0 0 0 12 23z" />
              <path fill="#FBBC05" d="M5.84 14.1a6.6 6.6 0 0 1 0-4.2V7.06H2.18a11 11 0 0 0 0 9.88l3.66-2.84z" />
              <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1A11 11 0 0 0 2.18 7.06l3.66 2.84C6.71 7.31 9.14 5.38 12 5.38z" />
            </svg>
            <span className="lp-glabel">Google로 계속하기</span>
            <span className="lp-spin" aria-hidden="true" />
          </button>

          <p className="lp-note">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <rect x="4.5" y="10.5" width="15" height="10" rx="2.5" />
              <path d="M8 10.5V7a4 4 0 0 1 8 0v3.5" />
            </svg>
            접근 권한이 없다면 관리자에게 초대를 요청하세요.
          </p>
        </main>

        <p className="lp-foot">© WISEnut · Playground Internal Platform</p>
      </div>
    </div>
  );
}

const LOGIN_CSS = `
.lp-root {
  --lp-surface:#ffffff; --lp-bg:#f4f4f3; --lp-line:#ececec; --lp-line-strong:#dedede;
  --lp-ink:#3a414e; --lp-ink-2:#6f7682; --lp-ink-3:#a2a7af; --lp-navy:#2f3744;
  --lp-indigo:#211d72; --lp-brand:#f26e21; --lp-brand-soft:#fdecdd;
  --lp-primary-ink:oklch(0.42 0.18 283); --lp-primary-soft:oklch(0.95 0.035 285);
  --lp-danger:#d64545; --lp-danger-soft:#fcecec; --lp-danger-ink:#b83636;
  --lp-radius:18px; --lp-radius-sm:12px;
  --lp-shadow-sm:0 1px 2px rgba(30,30,35,0.04), 0 1px 1px rgba(30,30,35,0.03);
  --lp-shadow-md:0 6px 22px -6px rgba(40,40,50,0.12), 0 2px 6px -2px rgba(40,40,50,0.06);
  --lp-shadow-card:0 22px 54px -26px rgba(50,45,40,0.22), 0 8px 22px -14px rgba(50,45,40,0.1);
  position:relative; min-height:100vh; width:100%;
  display:grid; place-items:center; padding:24px;
  background:var(--lp-bg); color:var(--lp-ink);
  font-family:"Pretendard Variable", Pretendard, "Inter Variable", -apple-system, system-ui, sans-serif;
  -webkit-font-smoothing:antialiased; letter-spacing:-0.01em;
}
.lp-root::before {
  content:""; position:fixed; inset:0; pointer-events:none; z-index:0;
  background:
    radial-gradient(620px 380px at 50% -6%, var(--lp-brand-soft), transparent 62%),
    radial-gradient(540px 360px at 100% 100%, oklch(0.96 0.015 60), transparent 64%);
  opacity:.7;
}
.lp-wrap { position:relative; z-index:1; width:100%; max-width:432px; display:flex; flex-direction:column; align-items:center; }
.lp-logo { display:inline-flex; align-items:center; gap:11px; }
.lp-mark { position:relative; width:38px; height:38px; border:3px solid var(--lp-indigo); border-radius:7px; flex-shrink:0; transform:rotate(-9deg); }
.lp-mark::after { content:""; position:absolute; left:6px; bottom:6px; width:10px; height:10px; border-radius:50%; background:var(--lp-brand); }
.lp-wm { font-size:27px; font-weight:800; letter-spacing:-0.03em; line-height:1; }
.lp-wm .lp-w { color:var(--lp-indigo); }
.lp-wm .lp-n { color:var(--lp-brand); }
.lp-panel { position:relative; width:100%; margin-top:30px; background:var(--lp-surface); border:1px solid var(--lp-line); border-radius:var(--lp-radius); box-shadow:var(--lp-shadow-card); padding:44px 46px 34px; }
.lp-lead { text-align:center; }
.lp-lead h1 { font-size:22px; font-weight:800; letter-spacing:-0.03em; color:var(--lp-indigo); }
.lp-lead p { margin-top:10px; font-size:14.5px; color:var(--lp-ink-2); line-height:1.55; text-wrap:balance; }
.lp-alert { display:flex; align-items:flex-start; gap:9px; margin-top:22px; padding:12px 14px; border-radius:var(--lp-radius-sm); background:var(--lp-danger-soft); border:1px solid color-mix(in oklch, var(--lp-danger) 26%, var(--lp-line)); font-size:13.5px; color:var(--lp-danger-ink); font-weight:600; line-height:1.45; }
.lp-alert svg { width:16px; height:16px; flex-shrink:0; margin-top:1px; }
.lp-gbtn { margin-top:28px; width:100%; height:56px; display:inline-flex; align-items:center; justify-content:center; gap:12px; border:1px solid var(--lp-line-strong); border-radius:var(--lp-radius-sm); background:#fff; font-family:inherit; font-size:15.5px; font-weight:700; color:#2a2a33; cursor:pointer; box-shadow:var(--lp-shadow-sm); transition:border-color .14s, background .14s, box-shadow .14s, transform .06s; }
.lp-gicon { width:20px; height:20px; flex-shrink:0; }
.lp-glabel { white-space:nowrap; }
.lp-gbtn:hover { border-color:var(--lp-ink-3); background:#fafafa; box-shadow:var(--lp-shadow-md); }
.lp-gbtn:active { transform:translateY(1px); }
.lp-gbtn:disabled { cursor:default; }
.lp-spin { width:18px; height:18px; border-radius:50%; border:2.5px solid var(--lp-primary-soft); border-top-color:var(--lp-primary-ink); animation:lp-spin .7s linear infinite; display:none; }
@keyframes lp-spin { to { transform:rotate(360deg); } }
.lp-gbtn.is-loading .lp-spin { display:inline-block; }
.lp-gbtn.is-loading .lp-glabel, .lp-gbtn.is-loading .lp-gicon { display:none; }
.lp-note { margin-top:26px; padding-top:20px; border-top:1px solid var(--lp-line); display:flex; align-items:center; justify-content:center; gap:8px; font-size:13px; color:var(--lp-ink-3); font-weight:500; line-height:1.5; }
.lp-note svg { width:14px; height:14px; flex-shrink:0; color:var(--lp-ink-3); }
.lp-foot { margin-top:26px; text-align:center; font-size:12.5px; color:var(--lp-ink-3); font-weight:500; letter-spacing:0.01em; }
@media (max-width:480px) {
  .lp-panel { padding:36px 26px 28px; }
  .lp-wm { font-size:24px; }
  .lp-lead h1 { font-size:20px; }
}
`;
