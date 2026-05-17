# db-sync-platform

`db-sync-platform`은 Source DB의 변경 데이터를 감지해 Redis Streams 또는 Kafka로 이벤트화하고, Elasticsearch/OpenSearch를 Primary Sink로 반영하는 Go 기반 DB 변경 데이터 동기화 파이프라인입니다.

초기 목표는 운영 DB의 row 변경을 검색 인덱스에 안정적으로 동기화하는 것입니다. 다만 Source Connector, Change Detector, Stream Backend, Sink를 분리해 향후 다른 DB, 변경 감지 전략, 대상 시스템으로 확장할 수 있도록 설계합니다.

## 목표

- Source DB에서 변경된 row를 감지합니다.
- 감지된 변경 데이터를 표준 `ChangeEvent`로 변환합니다.
- Redis Streams 또는 Kafka를 통해 변경 이벤트를 발행합니다.
- Elasticsearch/OpenSearch에 idempotent upsert/delete 방식으로 반영합니다.
- 실패 이벤트에 대해 retry와 DLQ를 제공합니다.
- cursor/checkpoint 저장을 통해 동기화 진행 위치를 관리합니다.
- Prometheus metrics로 동기화 상태, 실패, 지연을 관찰합니다.
- Docker Compose와 Kubernetes 배포 예시를 제공합니다.
- DB별 로직은 connector와 query contract 뒤로 격리합니다.

## 비목표

- Debezium 같은 완전한 CDC 도구를 대체하지 않습니다.
- 임의의 스키마에서 비즈니스 변경 규칙을 자동 추론하지 않습니다.
- 초기 버전에서 UI dashboard를 제공하지 않습니다.
- 초기 버전에서 모든 sink 유형을 지원하지 않습니다.
- 모든 시스템을 관통하는 exactly-once 처리를 보장하지 않습니다.

## 아키텍처

```text
+-------------------+
| Source DB         |
| MSSQL/Postgres/...|
+---------+---------+
          |
          | detect changes
          v
+-------------------+
| dbsync-poller     |
| ChangeDetector    |
+---------+---------+
          |
          | publish ChangeEvent
          v
+-----------------------------+
| Stream Backend              |
| Redis Streams or Kafka      |
+-------------+---------------+
              |
              | consume
              v
+-------------------+
| dbsync-worker     |
| Retry / DLQ       |
+---------+---------+
          |
          | apply event
          v
+-----------------------------+
| Primary Sink                |
| Elasticsearch / OpenSearch  |
+-----------------------------+
```

## 주요 컴포넌트

### Source Connector

Source Connector는 DB 접속과 사용자 정의 SELECT 쿼리 실행을 담당합니다.

초기 대상 connector:

- MSSQL
- PostgreSQL
- MySQL

첫 구현은 MSSQL부터 시작할 수 있지만, connector 경계는 MSSQL에 종속되지 않도록 유지합니다.

```go
type SourceConnector interface {
    Name() string
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    Close() error
}
```

### Change Detector

Change Detector는 query 결과를 표준 변경 이벤트로 변환합니다.

지원할 변경 감지 모드는 3가지입니다.

| Mode | 기준 | 필요한 데이터 | 사용 사례 |
|---|---|---|---|
| `timestamp` | 변경일시 | PK + changed_at | `UpdatedAt`, `UptDt` 등이 있는 일반 레거시 테이블 |
| `version` | 증가 버전/순번 | PK + version | `rowversion`, revision, sequence, change number 기반 |
| `snapshot` | hash 비교 | PK | timestamp/version 컬럼이 없는 테이블 |

### Stream Backend

감지된 이벤트는 Stream Backend로 발행합니다.

지원 대상:

- Redis Streams
- Kafka

두 backend는 동일한 이벤트 인터페이스 뒤에 둡니다.

```go
type EventPublisher interface {
    Publish(ctx context.Context, event ChangeEvent) error
}

type EventConsumer interface {
    Consume(ctx context.Context, handler EventHandler) error
}
```

패키지 방향:

```text
internal/stream/
├── redis/
│   ├── publisher.go
│   └── consumer.go
└── kafka/
    ├── publisher.go
    └── consumer.go
```

### Worker

Worker는 Redis Streams 또는 Kafka에서 `ChangeEvent`를 소비하고 Sink에 반영합니다.

처리 흐름:

```text
ChangeEvent consume
→ Elasticsearch/OpenSearch 반영
→ 성공 시 ack/commit
→ 실패 시 retry
→ 최대 재시도 초과 시 DLQ 이동
```

Worker는 at-least-once delivery를 전제로 동작합니다. 따라서 Sink write는 반드시 idempotent해야 합니다.

### Sink

Primary Sink는 Elasticsearch/OpenSearch입니다.

문서 ID는 entity 이름과 source entity ID를 기반으로 안정적으로 생성합니다.

```text
document_id = entity + ":" + entity_id
```

예시:

```text
hotel:10001
room_type:90021
product:50007
```

이렇게 하면 같은 이벤트가 중복 전달되어도 최종 문서 상태를 동일하게 유지할 수 있습니다.

## 변경 감지 모드

### Timestamp Mode

Timestamp Mode는 변경일시 컬럼을 기준으로 변경 데이터를 감지합니다.

대표 컬럼:

- `RegDt`
- `UptDt`
- `CreatedAt`
- `UpdatedAt`
- `ModifiedAt`
- `LastChangedAt`

필수 반환 컬럼:

```text
entity_id
changed_at
operation
```

Cursor 예시:

```json
{
  "changed_at": "2026-05-17T10:00:00Z",
  "entity_id": "10001"
}
```

MSSQL query 예시:

```sql
SELECT TOP (@limit)
    CAST(HotelId AS varchar(50)) AS entity_id,
    CASE
        WHEN UptDt IS NULL THEN RegDt
        WHEN UptDt > RegDt THEN UptDt
        ELSE RegDt
    END AS changed_at,
    'upsert' AS operation,
    HotelId,
    HotelName,
    Address,
    RegDt,
    UptDt
FROM dbo.Hotel
WHERE
    (
        CASE
            WHEN UptDt IS NULL THEN RegDt
            WHEN UptDt > RegDt THEN UptDt
            ELSE RegDt
        END > @last_changed_at
    )
    OR
    (
        CASE
            WHEN UptDt IS NULL THEN RegDt
            WHEN UptDt > RegDt THEN UptDt
            ELSE RegDt
        END = @last_changed_at
        AND HotelId > @last_entity_id
    )
ORDER BY changed_at, entity_id;
```

Cursor에 `changed_at`뿐 아니라 `entity_id`를 함께 사용하는 이유는 같은 timestamp를 가진 row가 여러 개 있을 수 있기 때문입니다.

### Version Mode

Version Mode는 증가하는 version 또는 sequence 값을 기준으로 변경 데이터를 감지합니다.

대표 컬럼:

- MSSQL `rowversion`
- revision
- sequence
- change_no
- monotonic version column

필수 반환 컬럼:

```text
entity_id
version
operation
```

Cursor 예시:

```json
{
  "version": "00000000000007D3"
}
```

Query 예시:

```sql
SELECT TOP (@limit)
    CAST(ProductId AS varchar(50)) AS entity_id,
    RowVer AS version,
    'upsert' AS operation,
    ProductId,
    ProductName,
    Price
FROM dbo.Product
WHERE RowVer > @last_version
ORDER BY RowVer;
```

Version Mode는 timestamp precision이나 clock skew 문제를 피할 수 있지만, 신뢰 가능한 증가 version 컬럼이 필요합니다.

### Snapshot Mode

Snapshot Mode는 timestamp 또는 version 컬럼이 없는 테이블을 위한 fallback 방식입니다.

필수 반환 컬럼:

```text
entity_id
```

Query 예시:

```sql
SELECT
    CAST(ProductId AS varchar(50)) AS entity_id,
    ProductId,
    ProductName,
    Price,
    Stock
FROM dbo.Product
ORDER BY ProductId;
```

처리 흐름:

```text
현재 row 조회
→ entity_id 기준 row hash 생성
→ 이전 snapshot과 비교
→ 신규 row: upsert
→ hash 변경 row: upsert
→ 사라진 row: delete
```

Snapshot Mode는 delete 감지가 가능하지만 incremental detection보다 비용이 크고 snapshot 저장소가 필요합니다.

## 사용자 정의 Query Contract

이 플랫폼은 테이블별 변경 규칙을 자동 추론하지 않습니다. 사용자가 테이블 구조에 맞는 SELECT 쿼리를 정의합니다.

단, query는 mode별 contract를 만족해야 합니다.

공통 규칙:

- SELECT query만 허용합니다.
- parameter binding을 사용합니다.
- 필수 반환 컬럼을 포함해야 합니다.
- 안정적인 정렬을 제공해야 합니다.
- limit 또는 paging 조건을 사용해야 합니다.
- query timeout을 강제해야 합니다.
- 운영 환경에서는 read-only DB 계정을 사용해야 합니다.

Mode별 parameter:

Timestamp Mode:

```text
@last_changed_at
@last_entity_id
@limit
```

Version Mode:

```text
@last_version
@limit
```

Snapshot Mode:

```text
@limit 또는 paging parameter
```

## ChangeEvent

Poller는 변경 row를 표준 `ChangeEvent`로 변환합니다.

```json
{
  "event_id": "evt_01H...",
  "source": "hotel-mssql",
  "entity": "hotel",
  "entity_id": "10001",
  "operation": "upsert",
  "changed_at": "2026-05-17T10:00:00Z",
  "version": "",
  "payload": {
    "HotelId": 10001,
    "HotelName": "ABC Hotel",
    "Address": "Seoul"
  }
}
```

지원 operation:

```text
upsert
delete
```

## 설정 예시

### Redis Streams

```yaml
version: 1

sources:
  - name: hotel-mssql
    driver: mssql
    dsn: ${MSSQL_DSN}

stream:
  type: redis
  redis:
    address: localhost:6379
    stream: db-sync-events
    consumer_group: db-sync-workers
    consumer_name: worker-1

sink:
  type: opensearch
  opensearch:
    addresses:
      - http://localhost:9200
    index: hotels
    document_id: "{{ .entity }}:{{ .entity_id }}"

entities:
  - name: hotel
    source: hotel-mssql
    detector:
      mode: timestamp
      cursor:
        changed_at: changed_at
        tie_breaker: entity_id
      query_file: ./queries/hotel_timestamp.sql

poller:
  interval: 10s
  batch_size: 500

retry:
  max_attempts: 5
  initial_backoff: 1s
  max_backoff: 30s

metrics:
  enabled: true
  address: :9100
```

### Kafka

```yaml
version: 1

sources:
  - name: hotel-mssql
    driver: mssql
    dsn: ${MSSQL_DSN}

stream:
  type: kafka
  kafka:
    brokers:
      - localhost:9092
    topic: db-sync-events
    consumer_group: db-sync-workers
    client_id: db-sync-platform

sink:
  type: elasticsearch
  elasticsearch:
    addresses:
      - http://localhost:9200
    index: hotels
    document_id: "{{ .entity }}:{{ .entity_id }}"

entities:
  - name: hotel
    source: hotel-mssql
    detector:
      mode: timestamp
      cursor:
        changed_at: changed_at
        tie_breaker: entity_id
      query_file: ./queries/hotel_timestamp.sql

poller:
  interval: 10s
  batch_size: 500

retry:
  max_attempts: 5
  initial_backoff: 1s
  max_backoff: 30s

metrics:
  enabled: true
  address: :9100
```

## 실행 흐름

### Poller

```text
1. entity 설정 로드
2. cursor 로드
3. 사용자 정의 query 실행
4. 필수 결과 컬럼 검증
5. row를 ChangeEvent로 변환
6. Redis Streams 또는 Kafka에 이벤트 발행
7. next cursor 저장
8. metrics 기록
```

### Worker

```text
1. Redis Streams 또는 Kafka에서 ChangeEvent consume
2. operation 확인
3. Elasticsearch/OpenSearch에 upsert/delete 반영
4. 성공 시 ack/commit
5. 실패 시 retry
6. 최대 재시도 초과 시 DLQ 이동
7. metrics 기록
```

## Delivery Semantics

이 플랫폼은 at-least-once delivery를 전제로 합니다.

같은 이벤트가 두 번 이상 처리될 수 있습니다. 따라서 Sink 계층은 idempotent해야 합니다.

원칙:

- 각 `ChangeEvent`는 고유한 `event_id`를 가집니다.
- Sink 문서 ID는 `entity`와 `entity_id`로 생성합니다.
- Upsert는 idempotent해야 합니다.
- Worker는 Sink write 성공 후에만 ack/commit합니다.
- 실패 이벤트는 retry 후 DLQ로 이동합니다.

## Delete 처리

Delete 처리는 감지 모드에 따라 다릅니다.

| Mode | Delete 감지 |
|---|---|
| Timestamp | `IsDeleted`, `DeletedAt`, `DelYn` 같은 soft-delete 컬럼이 있을 때 가능 |
| Version | delete marker 또는 delete event table이 있을 때 가능 |
| Snapshot | 이전 snapshot에는 있었지만 현재 snapshot에 없을 때 가능 |
| CDC | 향후 확장 |

첫 버전은 insert/update 동기화에 집중합니다. Delete 지원은 soft-delete 컬럼, snapshot diff, delete query, 또는 향후 CDC adapter로 확장합니다.

## Cursor 저장

Cursor는 source, entity, detector mode 단위로 저장합니다.

예시:

```json
{
  "source": "hotel-mssql",
  "entity": "hotel",
  "mode": "timestamp",
  "cursor": {
    "changed_at": "2026-05-17T10:00:00Z",
    "entity_id": "10001"
  }
}
```

초기 cursor 저장소 후보:

- File
- SQLite
- Redis
- PostgreSQL/MSSQL

초기 구현은 File 또는 SQLite로 시작하고 이후 외부 저장소로 확장할 수 있습니다.

## DLQ

DLQ는 retry 이후에도 처리하지 못한 이벤트를 보관합니다.

DLQ backend 후보:

- SQLite
- PostgreSQL
- Redis Streams DLQ stream
- Kafka DLQ topic

Redis Streams 예시:

```text
db-sync-events
db-sync-events-dlq
```

Kafka 예시:

```text
db-sync-events
db-sync-events-dlq
```

DLQ record 예시:

```json
{
  "event_id": "evt_01H...",
  "source": "hotel-mssql",
  "entity": "hotel",
  "entity_id": "10001",
  "operation": "upsert",
  "payload": {},
  "error": "opensearch timeout",
  "retry_count": 5,
  "failed_at": "2026-05-17T10:00:00Z"
}
```

향후 CLI 명령:

```bash
dbsyncctl dlq list
dbsyncctl dlq replay --id evt_01H...
```

## Metrics

Prometheus metrics:

```text
db_sync_polled_rows_total
db_sync_published_events_total
db_sync_consumed_events_total
db_sync_upsert_success_total
db_sync_upsert_failed_total
db_sync_dlq_total
db_sync_batch_duration_seconds
db_sync_sink_write_duration_seconds
db_sync_lag_seconds
```

`db_sync_lag_seconds`는 Sink가 Source 대비 얼마나 뒤처졌는지를 나타냅니다.

```text
db_sync_lag_seconds = now - last_processed_changed_at
```

## 프로젝트 구조

```text
.
├── cmd/
│   ├── dbsync-poller/
│   │   └── main.go
│   ├── dbsync-worker/
│   │   └── main.go
│   └── dbsyncctl/
│       └── main.go
├── internal/
│   ├── config/
│   ├── source/
│   │   ├── mssql/
│   │   ├── postgres/
│   │   └── mysql/
│   ├── detector/
│   │   ├── timestamp/
│   │   ├── version/
│   │   └── snapshot/
│   ├── stream/
│   │   ├── redis/
│   │   └── kafka/
│   ├── sink/
│   │   ├── elasticsearch/
│   │   └── opensearch/
│   ├── cursor/
│   ├── retry/
│   ├── dlq/
│   ├── metrics/
│   └── model/
├── queries/
│   └── hotel_timestamp.sql
├── configs/
│   ├── redis.example.yaml
│   └── kafka.example.yaml
├── deployments/
│   ├── docker-compose/
│   └── k8s/
├── docs/
│   ├── architecture.md
│   ├── detector-modes.md
│   ├── stream-backends.md
│   ├── failure-handling.md
│   └── operations.md
├── go.mod
└── README.md
```

## MVP 범위

첫 구현에 포함할 항목:

- MSSQL Source Connector
- Timestamp Mode Detector
- 사용자 정의 SELECT Query Contract
- Redis Streams Publisher/Consumer
- Kafka Publisher/Consumer
- Elasticsearch/OpenSearch Sink
- Retry Policy
- DLQ
- Prometheus Metrics
- Docker Compose 예시
- Kubernetes manifests

첫 구현에서 제외할 항목:

- 완전한 CDC 지원
- 자동 스키마 추론
- Web UI
- Multi-tenant 권한 관리
- 고급 Transformation DSL
- 대용량 Snapshot 최적화

## Roadmap

### Phase 1. Foundation

- [ ] 프로젝트 기본 구조 정의
- [ ] 설정 스키마 정의
- [ ] `ChangeEvent` 모델 정의
- [ ] Source Connector 인터페이스 정의
- [ ] Stream Publisher/Consumer 인터페이스 정의
- [ ] Sink 인터페이스 정의

### Phase 2. Timestamp Polling

- [ ] MSSQL connector 구현
- [ ] Timestamp query detector 구현
- [ ] 필수 결과 컬럼 검증
- [ ] Cursor 저장 구현
- [ ] Poller command 구현

### Phase 3. Stream Backends

- [ ] Redis Streams publisher 구현
- [ ] Redis Streams consumer 구현
- [ ] Kafka publisher 구현
- [ ] Kafka consumer 구현
- [ ] 설정 기반 Stream Backend 선택 처리

### Phase 4. Sink

- [ ] Elasticsearch sink 구현
- [ ] OpenSearch sink 구현
- [ ] 안정적인 document ID 생성 구현
- [ ] Bulk upsert 지원

### Phase 5. Failure Handling

- [ ] Retry policy 구현
- [ ] DLQ 저장 구현
- [ ] DLQ list command 구현
- [ ] DLQ replay command 구현

### Phase 6. Observability

- [ ] Prometheus metrics endpoint 제공
- [ ] Sync lag metric 추가
- [ ] Worker 성공/실패 metric 추가
- [ ] Grafana dashboard 예시 추가

### Phase 7. Deployment

- [ ] Dockerfile 추가
- [ ] Docker Compose 예시 추가
- [ ] Kubernetes manifests 추가
- [ ] ConfigMap/Secret 예시 추가
- [ ] Liveness/Readiness probe 추가

### Phase 8. Extended Detection Modes

- [ ] Version Mode 설계 확정
- [ ] Version Mode 구현
- [ ] Snapshot Mode 설계 확정
- [ ] Snapshot Store 구현
- [ ] Snapshot Diff 구현

## Kubernetes 배포 목표

예상 Kubernetes 컴포넌트:

```text
dbsync-poller   Deployment or CronJob
dbsync-worker   Deployment
redis/kafka      external dependency or dev manifest
opensearch       external dependency or dev manifest
prometheus       monitoring
grafana          dashboard
```

초기 manifest 구조:

```text
deployments/k8s/
├── namespace.yaml
├── configmap.yaml
├── secret.example.yaml
├── poller-deployment.yaml
├── worker-deployment.yaml
├── service.yaml
├── servicemonitor.yaml
└── kustomization.yaml
```

## 운영 고려사항

### 중복 이벤트

Stream 기반 처리는 이벤트를 중복 전달할 수 있습니다.

이 플랫폼은 다음 원칙으로 중복을 처리합니다.

```text
At-least-once delivery
+ stable document ID
+ idempotent sink writes
```

### Cursor 갱신 시점

Poller는 이벤트를 Stream Backend에 성공적으로 발행한 이후 next cursor를 저장합니다.

Sink write 실패는 Worker의 retry와 DLQ에서 별도로 처리합니다.

### Graceful Shutdown

Worker는 graceful shutdown을 지원해야 합니다.

```text
SIGTERM 수신
→ 신규 이벤트 consume 중단
→ 처리 중인 이벤트 완료 또는 timeout
→ 완료된 이벤트 ack/commit
→ 종료
```

### Backpressure

Sink가 느리거나 사용할 수 없는 상태가 되면 stream backlog가 증가합니다.

관찰해야 할 주요 신호:

```text
stream pending count
worker processing latency
sink write failure count
sync lag seconds
dlq count
```

## 개발 실행 예시

### Redis Streams

```bash
docker compose -f deployments/docker-compose/redis.yaml up -d

go run ./cmd/dbsync-poller --config configs/redis.example.yaml
go run ./cmd/dbsync-worker --config configs/redis.example.yaml
```

### Kafka

```bash
docker compose -f deployments/docker-compose/kafka.yaml up -d

go run ./cmd/dbsync-poller --config configs/kafka.example.yaml
go run ./cmd/dbsync-worker --config configs/kafka.example.yaml
```

## 설계 원칙

- Source DB, Stream Backend, Sink는 인터페이스로 분리합니다.
- 테이블별 변경 로직은 사용자 정의 SELECT 쿼리로 표현합니다.
- Query 결과는 mode별 명시적 contract를 만족해야 합니다.
- Worker는 at-least-once delivery를 전제로 합니다.
- Sink write는 idempotent해야 합니다.
- Cursor, Retry, DLQ는 운영 복구를 위한 핵심 구성요소로 취급합니다.
- Kubernetes 배포와 observability는 초기 설계에 포함합니다.
- Elasticsearch/OpenSearch를 Primary Sink로 삼되, Sink 경계는 확장 가능하게 유지합니다.

## License

MIT
