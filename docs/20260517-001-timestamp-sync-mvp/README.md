# timestamp-sync-mvp

## 요약
MSSQL 변경을 Timestamp Mode로 감지해 Redis Streams로 발행하고 OpenSearch에 반영하는 얇은 end-to-end 수직 슬라이스. 핵심 가치(DB 변경 → 검색 인덱스 동기화)를 동작으로 검증하고 이후 backend·sink·실패처리·관측을 얹을 골격을 세운다.

## 상태
- [x] SPEC
- [x] ANALYSIS
- [ ] IMPLEMENT

## 문서
- [spec.md](./spec.md)
- [analysis.md](./analysis.md) (ANALYSIS 단계에서 생성)
- [implement.md](./implement.md) (IMPLEMENT 단계에서 생성)

## 작업 히스토리
- 2026-05-17: SPEC 작성
- 2026-05-17: ANALYSIS 작성
- 2026-05-17: IMPLEMENT 체크리스트 작성
