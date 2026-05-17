# timestamp-sync-mvp SPEC

## 1. 범위

MSSQL Source DB의 row 변경을 Timestamp Mode로 감지해 Redis Streams로 이벤트화하고, OpenSearch에 반영하는 **얇은 end-to-end 수직 슬라이스**를 다룬다.

포함 영역:

- MSSQL Source Connector
- Timestamp Mode 변경 감지
- 사용자 정의 SELECT Query Contract (필수 결과 컬럼 검증 포함)
- delete 전략 두 가지: `soft_delete_in_query`, `none`
- Redis Streams publisher / consumer
- OpenSearch sink (안정적 document ID, external version 기반 조건부 쓰기)
- cursor 영속화
- poller / worker 두 실행 단위

이 feature는 "변경 한 건이 OpenSearch에 반영된다"가 실제로 도는 최소 골격을 닫는 것이 경계다. 이후 backend·sink·실패처리·관측은 이 골격에 붙이는 후속 feature로 분리한다.

## 2. 목표

- 이 플랫폼의 핵심 가치(운영 DB 변경 → 검색 인덱스 동기화)를 추론이 아니라 실제 동작으로 검증한다.
- Source / Stream / Sink 경계를 인터페이스로 분리한 골격을 세워, 이후 Kafka·Elasticsearch·retry/DLQ·metrics를 같은 골격 위에 증분으로 얹을 수 있게 한다.
- 스키마를 모르는 환경에서 delete 의미를 추론하지 않고, query 출력 contract와 entity별 선언으로 통제하는 방식이 실제로 성립함을 보인다.

## 3. 제약

- delivery는 at-least-once를 전제로 한다. Sink write는 idempotent해야 한다.
- document ID는 `entity` + `:` + `entity_id`로 안정적으로 생성한다. 같은 변경이 중복 전달돼도 최종 문서 상태가 동일해야 한다.
- out-of-order 방지를 위해 Sink write는 detector 정렬키에서 파생한 external version 기반 조건부 쓰기를 사용한다. Timestamp Mode에서 external version은 `changed_at`에서 파생한다.
- Timestamp Mode 정확성은 "entity별 `changed_at` 단조 비감소 + tie-breaker `entity_id`" 가정에 의존한다. 이 가정은 cursor 정확성과 external version의 공통 전제이며, 가정이 깨지는 소스는 이 mode 범위 밖이다.
- query는 사용자 정의 SELECT만 허용한다. parameter binding, 안정적 정렬, limit/paging 조건, query timeout 강제, 운영 환경 read-only DB 계정 사용을 전제로 한다.
- delete 의미는 entity별 선언값으로만 결정한다. 플랫폼은 임의 SQL 의미를 해석해 delete 의도를 추론하지 않는다.
  - `soft_delete_in_query`: query가 row별 `operation`으로 delete를 표현하며, delete 대상 row를 WHERE에서 제외하지 않음을 작성자가 보증한다.
  - `none`: query에 soft-delete 컬럼이 관여하지 않는 테이블(append-only 등) 대상. 항상 upsert만 발행한다.
- cursor는 source + entity + mode 단위로, 프로세스 재시작을 가로질러 영속되어야 한다.
- poller는 stream backend에 발행을 성공한 이후에만 next cursor를 저장한다.
- worker는 Sink write 성공 이후에만 ack/commit한다. SIGTERM 수신 시 처리 중 이벤트를 완료(또는 timeout)한 뒤 종료한다.
- 구현 언어는 Go, 설정은 YAML 파일로 받는다.

## 4. 제외 범위

- Kafka publisher / consumer
- Elasticsearch sink (이 feature는 OpenSearch만)
- retry policy, DLQ
- Prometheus metrics, sync lag metric
- Version Mode, Snapshot Mode 감지
- `snapshot_sweep` delete 전략 (README Roadmap Phase 8)
- `soft_delete_as_field` 등 추가 delete 전략 변형
- 완전한 CDC 지원, 자동 스키마 추론
- `dbsyncctl` CLI (DLQ list/replay 등)
- Web UI, multi-tenant 권한 관리, Transformation DSL
- 대용량 초기 적재(backfill)/reindex 최적화
- Docker Compose / Kubernetes manifests 산출물

## 5. 완료 조건

1. poller를 설정 파일과 함께 실행하면, 설정된 entity의 Timestamp Mode 쿼리를 cursor 이후 변경분에 대해 실행하고, 각 변경 row를 표준 `ChangeEvent`로 변환해 Redis Streams에 발행한다 (Redis stream에 해당 이벤트가 적재됨으로 관찰 가능).
2. worker를 같은 설정으로 실행하면, Redis Streams에서 `ChangeEvent`를 소비해 `operation: upsert`를 OpenSearch에 `entity:entity_id` document로 반영한다 (해당 ID의 OpenSearch 문서 존재·내용으로 관찰 가능).
3. `soft_delete_in_query`로 설정된 entity에서 `operation: delete`인 변경 이벤트가 처리되면, 해당 `entity:entity_id` 문서가 OpenSearch에서 삭제된다.
4. `none`으로 설정된 entity는 어떤 변경에 대해서도 `operation: delete` 이벤트를 발행하지 않으며, 항상 upsert만 발행한다.
5. 같은 `ChangeEvent`가 두 번 이상 전달되어 처리되어도 해당 OpenSearch 문서의 최종 상태가 한 번 처리한 경우와 동일하다.
6. 더 오래된 `changed_at`을 가진 이벤트가 더 최신 `changed_at`이 이미 반영된 문서보다 나중에 도착해 처리되어도, 최신 문서 내용이 더 오래된 내용으로 덮어써지지 않는다.
7. poller가 발행에 성공한 뒤 cursor를 저장하고, poller를 중단했다가 재시작하면 마지막 저장된 cursor 이후 변경분부터 폴링을 재개한다 (이미 발행된 변경의 중복 재발행은 허용되나, cursor 구간의 변경 누락은 없다).
8. 필수 결과 컬럼(`entity_id`, `changed_at`, `operation`)을 만족하지 못하는 사용자 정의 쿼리는, 변경을 발행하지 않고 명확한 에러로 거부된다 (조용히 진행하거나 부분 발행하지 않는다).
9. worker가 SIGTERM을 받으면, 신규 이벤트 소비를 멈추고 처리 중이던 이벤트를 완료(또는 timeout)한 뒤 ack/commit하고 종료한다. Sink write에 성공하지 못한 이벤트는 ack되지 않아 재소비 대상으로 남는다.
