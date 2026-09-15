---
name: taskrail-module-builder
description: taskrail(Background Job Runtime)의 core/queue-memory/job-runtime/cli/queue-driftmq 모듈을 정해진 순서로 구현할 때 사용. "JobQueue 인터페이스 만들어줘", "queue-memory adapter 구현", "worker pool 작성", "retry backoff 구현", "delayed job 만들어줘", "taskrail 다음 단계 구현", "taskrail 이어서 만들어줘" 같은 요청에 사용할 것. **설계 문서(README/docs/architecture.md/Public API)가 사용자 승인을 받기 전에는 이 스킬로 코드를 작성하지 않는다** — 승인 전이면 먼저 taskrail-architecture-design 스킬로 설계를 완성해야 한다. Scheduled Job(cron)/queue-postgres/DAG/분기 등 MVP 스코프 밖 기능은 이 스킬로 구현하지 않는다.
---

# taskrail 모듈 구현

taskrail은 `JobQueue` 인터페이스(enqueue/consume/ack/nack) 뒤에 여러 broker adapter를 꽂을 수 있는
**독립적인 Background Job Runtime**이다. driftmq는 그 중 하나의 선택적 adapter일 뿐이며, 코어(Job
정의, 상태 머신, worker pool, retry/timeout/concurrency)는 driftmq를 몰라도 완전히 동작해야 한다.

## 시작 전 필수 확인

`README.md`, `docs/architecture.md`, `docs/job-lifecycle.md`, `docs/failure-model.md`가 존재하고
**사용자 승인을 받았는지** 먼저 확인한다. 이 문서들이 없거나 승인 전이라면, 먼저
`taskrail-architecture-design` 스킬(또는 `taskrail-architect` 에이전트)을 실행해야 한다 — 승인 없이
구현을 시작하지 않는다. Public API 초안이 존재하면 그 시그니처를 그대로 따른다 — 구현 중 임의로
바꾸지 않는다.

## 구현 순서

순서를 건너뛰지 않는다. 각 단계는 이전 단계의 산출물(`JobQueue` 계약, Job 상태 타입)에 의존한다.

1. **`packages/core` — `JobQueue` 인터페이스 + Job 정의 API**: `enqueue(job)`, `consume(handler)`,
   `ack(jobId)`, `nack(jobId)`, `taskrail.job(name, handler)`. Job 상태 머신 타입:
   `WAITING → RUNNING → SUCCESS`, 실패 시 `FAILED → RETRY_WAIT → WAITING`(retry 가능) 또는
   `FAILED → DEAD`(retry 소진). 설계 문서의 시그니처를 그대로 옮긴다.
2. **`packages/queue-memory`**: `JobQueue`의 메모리 기반 구현체. driftmq 같은 외부 의존성 없이 코어
   계약이 실제로 성립하는지 여기서 먼저 검증한다.
3. **Job runtime**:
   - worker pool — 설정된 concurrency 이상 동시 실행하지 않는다 (`Queue > Worker` backpressure)
   - retry + backoff — 설계 문서의 정책(지수 백오프 등)을 그대로 구현
   - timeout — handler가 제한 시간을 넘기면 실패로 처리
   - delayed job — 지정 지연 시간이 지나야 실행 (cron 기반 scheduled job과 다르다, 이건 MVP 아님)
   - job history — 상태 전이 이력 기록
   - graceful shutdown — 진행 중인 job을 안전하게 마무리하거나 재큐잉
4. **CLI/최소 관측 API**: job 제출, jobId로 상태/이력 조회. `job.enqueued`/`started`/`completed`/
   `failed`/`retried`/`dead` 이벤트 훅(설계 문서에 정의된 범위만).
5. **`packages/queue-driftmq`**: driftmq adapter. 시작 전 `_workspace/driftmq_adapter/00_findings.md`를
   읽는다(재조사 불필요, 소스 기반 확인 완료). 새 질문이 생기면
   `driftmq-adapter-investigation` 스킬로 추가 조사한다. **driftmq의 transport-level retry와
   taskrail의 application-level retry를 중복 구현하지 않는다** — `docs/failure-model.md`의 경계를
   그대로 따른다.

## 스코프 경계 — 이 스킬로 절대 구현하지 않는 것

- Scheduled Job(cron 기반 트리거) — Delayed Job(단발 지연)과 구분
- `packages/queue-postgres` — 생태계 문서의 후보 adapter지만 MVP 필수 목록엔 없음
- 분기(branching)/DAG, 보상 트랜잭션(compensation) — taskrail MVP 책임 목록에 없음
- driftmq/beacon이 이미 하는 브로커·토픽 레벨 메트릭, WebSocket 실시간 전달의 재구현

구현 중 이런 아이디어가 떠오르면 코드에 넣지 말고 `_workspace/deferred_ideas.md`에 한 줄로 메모만
남기고 계속 진행한다.

## 원칙

- 설계 문서가 정한 언어 스택을 그대로 따른다.
- taskrail은 At-Least-Once Job Execution만 보장한다 — "정확히 한 번"을 흉내 내는 코드를 만들지
  않는다. 대신 handler가 idempotent하게 작성되기 쉽도록 API를 단순하게 유지한다.
- 경계를 넘는 값(Job 상태 필드명, 이벤트 이름 등)의 문자열은 한 곳(상수/타입)에서 정의하고 양쪽이
  그 상수를 참조하게 한다 — 문자열 리터럴을 양쪽에 따로 쓰면 오타로 깨지기 쉽고, QA가 가장 먼저
  잡아내는 버그 유형이다.
- 각 단계 완료 후 무엇을 했는지 몇 줄로 짧게 보고한다.

## 재호출 시

이전에 구현한 모듈이 있으면 다시 쓰지 않고 읽은 뒤, QA 리포트나 사용자 피드백이 지목한 부분만
수정한다. 설계 문서가 리뷰를 거쳐 갱신되었다면, 갱신된 부분과 현재 코드의 차이를 먼저 확인한다.
