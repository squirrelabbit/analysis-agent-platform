# Web Console

`apps/web`는 Analysis Support Platform의 운영 콘솔 프론트엔드 스캐폴드다.

## 실행

```bash
cd /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/web
npm install
npm run dev
```

기본 개발 서버 포트는 `4173`이다.

## API 연결

- 기본 control plane 주소: `http://127.0.0.1:18080`
- Vite dev proxy: `/api/* -> control plane`

`.env.example`:

```bash
VITE_API_BASE_URL=http://127.0.0.1:18080
```

## 현재 범위

- 운영 콘솔 시작 화면
- Dataset / Scenario / Execution 우선 구현 기준 제시
- control plane API 연결을 위한 최소 Vite proxy 설정

확인 필요:
- 실제 라우팅 구조와 상태 관리 라이브러리 선택은 다음 단계에서 확정한다.
