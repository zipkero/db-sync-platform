# timestamp-sync-mvp IMPLEMENT

> 순수 실행 체크리스트. 각 Task는 자체 검증 조건을 가진 최소 단위다. 설계 근거는 analysis.md, 요구사항 레벨 완료 조건은 spec.md §5에 있다.
> 순서는 문서 내 위치(line order)로 표현한다. ID는 영구 식별자이며 재번호하지 않는다.

## Section: 기반 (Model / Config)

- [ ] task-001: 프로젝트 골격 및 Go 모듈 초기화
  - 목적: 빈 저장소에서 두 실행 엔트리포인트(poller, worker)가 빌드되어 실행 가능한 골격이 존재한다
  - 접근: `go.mod` 생성, poller/worker 두 엔트리포인트와 내부 패키지 경계(model/config/source/detector/cursor/stream/sink) 디렉토리 골격 생성
  - 검증 조건:
    - 결과: 두 엔트리포인트가 컴파일되고 빈 실행 시 설정 누락 등 명시적 에러로 종료
    - 확인: `go build ./...` 성공, 두 엔트리포인트 실행 시 명확한 에러 메시지로 종료
  - 참조: SPEC §5.1, SPEC §5.2, ANALYSIS §1

- [ ] task-002: ChangeEvent 및 cursor 공통 타입 정의
  - 목적: poller가 생성하고 stream을 통과해 worker·sink가 소비하는 표준 이벤트와 cursor 표현이 공통 타입으로 존재한다
  - 접근: `source`/`entity`/`entity_id`/`operation`/`changed_at`/external version/`payload`/이벤트 고유 식별자 필드를 가진 ChangeEvent와 (changed_at, entity_id) cursor 타입 정의, document ID는 `entity:entity_id`로 파생하는 함수 포함
  - 검증 조건:
    - 결과: ChangeEvent 직렬화/역직렬화가 왕복 보존되고 document ID가 `entity:entity_id`로 안정적으로 생성됨
    - 확인: 단위 테스트로 직렬화 왕복과 document ID 생성 안정성 검증
  - 참조: SPEC §5.2, SPEC §5.5, ANALYSIS §3

- [ ] task-003: YAML Config 로더 구현
  - 목적: 동일한 YAML 설정 파일에서 sources / stream / sink / entities / poller 설정을 읽어 구조화한다
  - 접근: entity별 detector mode, cursor 컬럼 매핑, query 파일 경로, delete 전략 선언값(`soft_delete_in_query`|`none`)을 포함한 설정 구조체와 YAML 파서 구현
  - 검증 조건:
    - 결과: 유효 YAML이 구조체로 로드되고, delete 전략 선언값과 cursor 컬럼 매핑이 entity별로 보존됨. 누락·미지원 값은 명시적 에러
    - 확인: 단위 테스트로 유효 설정 로드와 잘못된/누락 필드 거부 검증
  - 참조: SPEC §5.3, SPEC §5.4, ANALYSIS §1

## Section: Poller 경로

- [ ] task-004: MSSQL Source connector 구현
  - 목적: 설정된 entity의 사용자 정의 SELECT query를 cursor 파생 파라미터로 실행해 결과 row 집합을 돌려준다
  - 접근: Source 인터페이스 정의 후 MSSQL 구현체 작성. parameter binding, 안정적 정렬, limit/paging, query timeout, read-only 계정 사용을 호출 계약으로 전제
  - 검증 조건:
    - 결과: 사용자 정의 SELECT가 cursor 이후 변경분 파라미터로 실행되어 row 집합 반환
    - 확인: 인터페이스 경계에 대한 단위 테스트(파라미터 바인딩·정렬 계약), 실 MSSQL 대상 수동 확인
  - 참조: SPEC §5.1, ANALYSIS §1, ANALYSIS §3

- [ ] task-005: Timestamp Mode Detector 구현 (필수 컬럼 검증 + operation 결정 + external version 파생 + next cursor)
  - 목적: source row를 표준 ChangeEvent로 변환하되, 필수 결과 컬럼이 없으면 변경을 발행하지 않고 명확한 에러로 거부하며, entity의 delete 전략 선언값으로 operation을 확정하고 정렬키에서 단조 비감소 external version과 next cursor를 산출한다
  - 접근: Detector(mode) 인터페이스 정의 후 Timestamp Mode 구현체 작성. `entity_id`/`changed_at`/`operation` 누락 시 배치/쿼리 단위 거부(부분 발행 없음). `none`은 row operation 무시하고 항상 upsert, `soft_delete_in_query`는 row `operation` 신뢰. external version은 `(changed_at, entity_id)` 정렬키에서 파생한 단조 비감소 값으로 인코딩(정확성 불변 인코딩 디테일은 구현 시 결정)
  - 검증 조건:
    - 결과: 필수 컬럼 누락 쿼리는 0건 발행 + 에러; `none` entity는 모든 row가 upsert; `soft_delete_in_query` entity는 row operation대로 upsert/delete; 같은 `changed_at` 다중 row에서 tie-breaker가 version에 반영되어 단조 비감소; next cursor가 마지막 발행 row의 (changed_at, entity_id)
    - 확인: 단위 테스트로 필수 컬럼 누락 거부·부분발행 없음, `none`/`soft_delete_in_query` operation 분기, 동일 timestamp tie-breaker version 단조성, next cursor 계산 검증
  - 참조: SPEC §5.3, SPEC §5.4, SPEC §5.6, SPEC §5.8, ANALYSIS §2, ANALYSIS §5

- [ ] task-006: File 기반 Cursor store 구현
  - 목적: (source, entity, mode) 키 cursor를 프로세스 재시작을 가로질러 영속하고 load/save를 제공한다
  - 접근: Cursor store 인터페이스 정의 후 File 기반 구현체 작성. save는 발행 성공 이후에만 호출되는 계약으로 둠
  - 검증 조건:
    - 결과: save 후 새 프로세스(재인스턴스)에서 load 시 마지막 저장 cursor가 그대로 반환됨
    - 확인: 단위 테스트로 save→재load 영속성, 미존재 키의 초기 cursor 동작 검증
  - 참조: SPEC §5.7, ANALYSIS §1, ANALYSIS §5

- [ ] task-007: Redis Streams publisher 구현
  - 목적: ChangeEvent를 Redis Streams에 발행하고 성공/실패를 호출 측에 반환한다
  - 접근: Stream publisher 인터페이스 정의 후 Redis Streams 구현체 작성. 발행 결과를 cursor 저장 전제로 노출
  - 검증 조건:
    - 결과: 발행 성공 시 Redis stream에 해당 ChangeEvent가 적재되어 관찰 가능, 실패 시 실패가 반환됨
    - 확인: 인터페이스 단위 테스트 + 실 Redis 대상 수동 발행/조회 확인
  - 참조: SPEC §5.1, ANALYSIS §1, ANALYSIS §3

- [ ] task-008: Poller 실행 단위 조립 (발행 성공 후 cursor 저장)
  - 목적: poller를 설정 파일과 함께 실행하면 entity별 cursor 이후 변경분을 쿼리·변환·발행하고, 발행에 성공한 뒤에만 next cursor를 저장하며 발행 실패 시 cursor를 전진시키지 않는다
  - 접근: Config→Source→Detector→Cursor store→Publisher 조립. 발행 성공 분기에서만 cursor save 호출, 실패 시 미저장으로 다음 폴링 재시도
  - 검증 조건:
    - 결과: 정상 경로에서 변경 row가 Redis stream에 적재되고 cursor가 (changed_at, entity_id)로 전진; 발행 실패 시 cursor 미전진; 중단 후 재시작 시 마지막 저장 cursor 이후부터 폴링 재개(누락 없음, 중복 재발행 허용)
    - 확인: poller 통합 동작 수동 확인(발행→cursor 전진, 발행 실패 주입 시 미전진, 재시작 재개) + task-012 테스트로 비선형 시점 회귀 검증
  - 참조: SPEC §5.1, SPEC §5.7, ANALYSIS §2, ANALYSIS §5

## Section: Worker 경로

- [ ] task-009: Redis Streams consumer 구현 (consumer group + 미-ack 재소비 + graceful 종료)
  - 목적: Redis Streams에서 ChangeEvent를 핸들러에 전달하고 핸들러 성공 시에만 ack/commit하며, SIGTERM 시 신규 소비를 멈추고 처리 중 이벤트를 완료(또는 timeout)한 뒤 종료한다
  - 접근: Stream consumer 인터페이스 정의 후 Redis Streams 구현체 작성. consumer group/pending 재소비, 핸들러 성공 시에만 ack, SIGTERM 시 신규 consume 중단 + 처리 중 완료/timeout 후 종료
  - 검증 조건:
    - 결과: 핸들러 실패 이벤트는 ack되지 않아 pending으로 잔존(재소비 대상); SIGTERM 시 신규 consume 중단 후 처리 중 이벤트만 완료/timeout하고 종료
    - 확인: 인터페이스 단위 테스트로 핸들러 실패 시 미-ack·pending 잔존, SIGTERM 분기 동작 검증 + 실 Redis 수동 확인
  - 참조: SPEC §5.9, ANALYSIS §2, ANALYSIS §3

- [ ] task-010: OpenSearch sink 구현 (external version 조건부 upsert/delete + idempotency)
  - 목적: ChangeEvent를 `entity:entity_id` document에 반영하되, external version 조건부 쓰기로 더 오래된 이벤트가 최신 문서를 덮어쓰지 않게 하고 같은 이벤트 중복 처리에 idempotent하게 동작한다
  - 접근: Sink 인터페이스 정의 후 OpenSearch 구현체 작성. `upsert`는 external version 조건부 색인, `delete`는 external version 조건부 삭제, document ID는 task-002의 `entity:entity_id` 파생 사용
  - 검증 조건:
    - 결과: upsert 이벤트가 `entity:entity_id` 문서로 반영(존재·내용 관찰 가능); delete 이벤트가 해당 문서 삭제; 같은 이벤트 2회 이상 처리해도 최종 문서 상태 동일; 더 오래된 `changed_at` 이벤트가 최신 반영 문서를 덮어쓰지 않음
    - 확인: 인터페이스 단위 테스트로 조건부 쓰기·idempotency·out-of-order 거부 + 실 OpenSearch 대상 수동 확인
  - 참조: SPEC §5.2, SPEC §5.3, SPEC §5.5, SPEC §5.6, ANALYSIS §2, ANALYSIS §3

- [ ] task-011: Worker 실행 단위 조립 (sink write 성공 후 ack + SIGTERM 분기)
  - 목적: worker를 같은 설정으로 실행하면 Redis Streams에서 ChangeEvent를 소비해 OpenSearch에 반영하고 sink write 성공 이후에만 ack/commit하며, SIGTERM 시 처리 중 이벤트만 완료(또는 timeout)한 뒤 종료하고 sink write에 성공 못한 이벤트는 ack되지 않는다
  - 접근: Config→Consumer→Sink 조립. consumer 핸들러에서 sink write 성공 시에만 ack 반환, SIGTERM 시 task-009 graceful 경로로 위임
  - 검증 조건:
    - 결과: upsert/delete 이벤트가 OpenSearch에 반영되고 성공 시 ack; sink write 실패 이벤트는 미-ack로 재소비 대상 잔존; SIGTERM 시 처리 중 이벤트 완료/timeout 후 ack하고 종료, 미성공 이벤트는 미-ack
    - 확인: worker 통합 동작 수동 확인(소비→반영→ack, sink 실패 주입 시 미-ack, SIGTERM 분기) + task-013 테스트로 비선형 시점 회귀 검증
  - 참조: SPEC §5.2, SPEC §5.3, SPEC §5.9, ANALYSIS §2, ANALYSIS §5

## Section: 회귀 테스트

- [ ] task-012: Poller 발행-성공-후-cursor-저장 경계 테스트 작성
  - 목적: 발행 성공 시에만 cursor 전진하고 발행 실패 시 미전진하며 재시작 시 누락 없이 재개하는 동작 회귀 방지
  - 접근: integration 레벨 — fake/stub publisher·cursor store로 발행 성공/실패 분기와 재시작 재개를 커버
  - 검증 조건:
    - 결과: 발행 실패 시 cursor 미전진, 발행 성공 시 (changed_at, entity_id) 전진, 재시작 시 마지막 저장 cursor 이후 재개·구간 누락 없음 케이스가 커버됨
    - 확인: 로컬/CI에서 해당 테스트 통과
  - 참조: SPEC §5.7

- [ ] task-013: Worker sink-write-성공-후-ack 및 SIGTERM 분기 테스트 작성
  - 목적: sink write 성공 시에만 ack, 실패 시 미-ack 잔존, SIGTERM 시 처리 중만 완료/timeout 후 종료하는 동작 회귀 방지
  - 접근: integration 레벨 — fake/stub consumer·sink로 ack 분기와 SIGTERM graceful 경로를 커버
  - 검증 조건:
    - 결과: sink 실패 이벤트 미-ack·재소비 대상 잔존, 성공 이벤트 ack, SIGTERM 시 신규 소비 중단 + 처리 중 완료/timeout 후 종료·미성공 미-ack 케이스가 커버됨
    - 확인: 로컬/CI에서 해당 테스트 통과
  - 참조: SPEC §5.9
