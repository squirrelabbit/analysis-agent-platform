# silverone 2026-06-01 — web(Vite+React SPA) 빌드 + nginx 정적 serve.
# 브라우저는 web 컨테이너만 호출하는 동일출처 구조다 — axios baseURL을 `/api`로
# 빌드하고 nginx가 `/api/*`를 control-plane으로 프록시한다 (nginx.conf 참조).
#
# build context는 ./apps/web (compose.dev.yml의 web 서비스 참조).

# 1) build stage — vite build → /app/dist
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# 빌드 시점에 axios baseURL(prod)로 박히는 값. 기본 `/api` = nginx 동일출처
# 프록시 경로. compose 밖에서 빌드해도 사용자 PC(127.0.0.1)를 때리지 않는다.
# 절대주소가 필요한 배포만 build-arg로 덮는다.
ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
RUN npm run build

# 2) serve stage — nginx 정적 serve
FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
