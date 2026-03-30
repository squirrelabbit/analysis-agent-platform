# Rust Skill Worker

이 디렉터리는 목표 아키텍처에서 Rust가 맡을 고성능 Skill worker 스캐폴드다.

## 책임

- clustering
- deduplication
- cooccurrence
- 대용량 텍스트 전처리

## 원칙

- 제품 정책과 orchestration은 여기서 처리하지 않는다.
- hot path로 확인된 Skill만 Rust로 올린다.
