---
name: taskrail-architecture-design
description: taskrail(Background Job Runtime)의 architecture 문서와 Public API 초안을 작성할 때 사용. "taskrail 설계 문서 작성", "taskrail architecture.md 만들어줘", "taskrail Public API 초안", "taskrail 언어 스택 결정", "taskrail 설계 리뷰 준비" 같은 요청에 사용할 것. 코드 구현 전 단계이며, 이 스킬로 만든 산출물은 반드시 사용자 리뷰를 거쳐야 구현(taskrail-module-builder)으로 넘어간다.
---

# taskrail architecture 설계

taskrail은 driftmq에 종속된 상태 추적기가 아니라 **독립적인 Background Job Runtime**이다
(`docs/reference/ecosystem-architecture-source.md` 2절). driftmq는 `JobQueue` 인터페이스 뒤의
선택적 adapter 중 하나일 뿐이다. 이 스킬의 산출물이 확정되기 전에 코드를 쓰면, 이전 taskrail 설계가
정체성째로 뒤집혔던 것과 같은 재작업을 반복하게 된다 — 그래서 설계 문서와 Public API를 먼저 고정하고
사람이 리뷰하는 순서를 지킨다(생태계 문서 15절).

## 반드시 답해야 할 질문 (생태계 문서 14절)

명확한 답을 찾지 못한 기능은 MVP에 포함하지 않는다.

- 왜 BullMQ가 아닌 Taskrail인가? / 왜 Temporal이 아닌 Taskrail인가?
- Job retry와 broker redelivery의 차이는 무엇인가?
- Worker crash를 어떻게 감지하는가?
- 중복 Job 실행을 어떻게 다루는가?
- idempotency를 어떻게 지원할 것인가?

## 책임 경계를 문서에 명시한다

생태계 문서 2절 "DriftMQ Integration"이 정한 경계를 그대로 가져온다:

```
DriftMQ = transport-level retry (네트워크 실패 등)
Taskrail = application-level retry policy (Payment API 503 등)
```

이 경계가 모호하면 driftmq adapter 구현 단계에서 재시도가 중복되거나 유실되는 버그로 이어진다 —
`docs/failure-model.md`에 구체적 시나리오(예: "네트워크 실패 → driftmq redelivery", "핸들러 내부
예외 → taskrail retry")로 적는다.

## 언어 스택 결정 (생태계 문서 11절)

Rust는 제외. TypeScript/Node.js, Java, Go 중 throughput, memory usage, GC behavior, concurrency
model, binary/deployment simplicity, library ecosystem, contributor accessibility, maintenance
cost 기준으로 비교표를 작성한다. 단순히 빠르다는 이유만으로 고르지 않는다.

**이 저장소 고유의 제약**: driftmq 실제 클라이언트는 Java로 확인되어 있다
(`_workspace/driftmq_adapter/00_findings.md`). taskrail이 Java가 아닌 언어를 선택하면
`queue-driftmq` adapter를 어떻게 구현할지(자체 재구현 / 별도 프로세스 + IPC / 다른 방법)를 비교표
옆에 명시해야 한다. 이 트레이드오프를 숨기지 않는다 — 사용자 리뷰에서 가장 먼저 질문받을 지점이다.

## 산출물 체크리스트

- `README.md` — taskrail이 무엇인지, 왜 존재하는지, `taskrail.job()` 정의부터 worker 실행까지 최소
  예시
- `docs/architecture.md` — component diagram, `JobQueue` 인터페이스(enqueue/consume/ack/nack)와
  adapter 구조(core/queue-memory/queue-driftmq — queue-postgres는 MVP 아님, 후보로만 언급), Job
  상태 머신(`WAITING → RUNNING → SUCCESS / FAILED → RETRY_WAIT → WAITING`, 소진 시 `DEAD`)
- `docs/job-lifecycle.md` — 상태 전이 규칙, retry backoff 정책, timeout, delayed job 동작
- `docs/failure-model.md` — driftmq/taskrail retry 경계, idempotent handler 권장 이유,
  at-least-once job execution의 의미
- Public API 초안 — `taskrail.job(name, handler)`, enqueue, `JobQueue` 타입 시그니처 (구현 아님,
  시그니처만)

## MVP 스코프 표를 문서에 포함한다

| 구분 | 항목 |
|------|------|
| 필수 | Job define/enqueue, Worker, Retry, Timeout, Concurrency, Delayed Job, Job State, Memory adapter, DriftMQ adapter |
| MVP 아님 | Scheduled Job(cron), `queue-postgres` adapter, Beacon 연동/통합 데모 |

## 사용자 리뷰 게이트 — 이 순서를 건너뛰지 않는다

산출물 초안이 끝나면 다음 10개 항목으로 정리해 사용자에게 제시하고 승인을 받는다 (생태계 문서 15절
Phase 3 형식):

1. Architecture 2. Component diagram 3. Job lifecycle 4. Retry/backoff/timeout 정책
5. Failure scenarios 6. Storage/queue 모델 7. Public API 8. MVP scope 9. Non-goals
10. 주요 기술적 trade-off (언어 선택, driftmq adapter 구현 방식)

승인 전에는 `taskrail-module-builder` 스킬(구현)로 넘어가지 않는다. 리뷰에서 모호하다는 피드백을
받은 항목은 코드가 아니라 문서를 먼저 구체화한다.

## 재호출 시

이전 설계 문서가 있으면 먼저 읽고, 사용자 피드백이나 구현 단계에서 올라온 질문이 지목한 부분만
수정한다. Public API를 바꿀 때는 문서와 (이미 구현된) 코드가 갈라지지 않도록 변경 범위를 명확히
표시한다.
