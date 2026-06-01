# silverone 2026-06-01 — web(Vite+React SPA) 빌드 + nginx 정적 serve (Option A).
# 브라우저는 control-plane(18080)을 직접 호출하고 CORS는 control-plane 기본
# 허용 origin(127.0.0.1:5173 / localhost:5173)을 사용한다. reverse-proxy/Go
# embed 없이 정적 serve만 한다.
#
# build context는 ./apps/web (compose.dev.yml의 web 서비스 참조).

# 1) build stage — vite build → /app/dist
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# 빌드 시점에 axios baseURL(prod)로 박히는 control-plane 주소. compose에서 주입.
ARG VITE_API_BASE_URL=http://127.0.0.1:18080
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
RUN npm run build

# 2) serve stage — nginx 정적 serve
FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
