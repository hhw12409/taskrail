# Taskrail

**A broker-agnostic Background Job Runtime for TypeScript/Node.js.**

[English](#english) | [한국어](#한국어)

> **Status: MVP complete.** All five packages (core, worker runtime, memory adapter, DriftMQ
> adapter) are implemented and tested. Not published to npm yet.

---

## English

Taskrail lets you say "run this function later, safely" and manages the execution lifecycle for
you — worker pool, retry, backoff, timeout, concurrency, delayed jobs, and job state.

```text
Application
    │  define with taskrail.job(), request with .enqueue()
    ▼
 Taskrail Core ── JobQueue (interface)
    │                 ├─ @taskrail/queue-memory
    │                 └─ @taskrail/queue-driftmq
    ▼
 Worker Pool
    │
    ▼
 Handler
```

### Why Taskrail

- **You don't have to pick a queue up front.** Core depends only on the `JobQueue` interface.
  Tests and single-process apps use the memory adapter; production uses the DriftMQ adapter (or
  another one later). The code doesn't change. Taskrail **does not require DriftMQ** as a hard
  dependency.
- **A BullMQ alternative.** BullMQ is locked to Redis, and job semantics are baked into Redis Lua
  scripts. Taskrail is broker-agnostic — the memory adapter runs the **same state machine** as
  production, so retry/timeout/DEAD paths are unit-testable without a Redis container.
- **The opposite of Temporal, not a competitor to it.** Temporal is a durable workflow engine
  (deterministic replay, a separate cluster). Taskrail is **not a workflow engine** — no DAGs, no
  branching, no compensation transactions. One job = one handler call. A library, not a server.
- **Honest about guarantees.** Taskrail provides **at-least-once job execution**. Duplicate runs
  can happen; write idempotent handlers (`ctx.jobId` is stable across every attempt and can be
  used as an idempotency key).

### Install

```sh
npm install @taskrail/core @taskrail/queue-memory
```

> Not published yet — this is the intended install once the first release ships.

### Quick start

```ts
// jobs.ts — define a job
import { Taskrail } from "@taskrail/core";
import { MemoryQueue } from "@taskrail/queue-memory";

export const taskrail = new Taskrail({ queue: new MemoryQueue() });

export const sendEmail = taskrail.job(
  "send-email",
  async (payload: { userId: string }, ctx) => {
    // ctx.jobId is stable across attempts — use it as an idempotency key
    await mailer.send(payload.userId, { idempotencyKey: ctx.jobId });
  },
  { maxAttempts: 5, timeoutMs: 10_000 },
);
```

```ts
// api.ts — enqueue (e.g. inside a web request handler)
await sendEmail.enqueue({ userId: "1234" });
await sendEmail.enqueue({ userId: "1234" }, { delayMs: 60_000 }); // delayed job
```

```ts
// worker.ts — run it
import { taskrail } from "./jobs";

const worker = taskrail.createWorker({ concurrency: 10 });
await worker.start();

process.on("SIGTERM", () => worker.stop()); // graceful shutdown
```

Switching to DriftMQ in production is a **one-line adapter swap**:

```ts
import { DriftmqQueue } from "@taskrail/queue-driftmq";

export const taskrail = new Taskrail({
  queue: new DriftmqQueue({ host: "127.0.0.1", port: 9092 }),
});
```

### MVP scope

**In:** job define/enqueue, worker pool, retry + backoff, timeout, concurrency, delayed jobs, job
state, graceful shutdown, `@taskrail/queue-memory`, `@taskrail/queue-driftmq`

**Out (for now):** scheduled/cron jobs, `@taskrail/queue-postgres`, Beacon integration,
CLI/dashboard, DAGs/workflows, priority queues, rate limiting

### Packages

| Package | Status |
|---|---|
| `@taskrail/core` | done |
| `@taskrail/queue-memory` | done |
| `@taskrail/queue-driftmq` | done |

### Contributing

Issues and PRs are welcome once the first release is out. The project is still moving fast at the
API-surface level, so please open an issue before a large PR to avoid wasted work.

### License

[MIT](LICENSE)

---

## 한국어

Taskrail은 "이 함수를 나중에, 안전하게 실행해줘"라고만 말하면 되는 **broker-agnostic Background Job
Runtime**이다. worker pool, retry, backoff, timeout, concurrency, delayed job, job state 같은 실행
lifecycle을 대신 관리한다.

### 왜 Taskrail인가

- **큐를 먼저 고르지 않아도 된다.** core는 `JobQueue` 인터페이스에만 의존한다. 테스트와 단일
  프로세스는 memory adapter, 프로덕션은 DriftMQ adapter를 쓴다. 코드는 그대로다. Taskrail은
  DriftMQ를 **필수 의존성으로 갖지 않는다.**
- **BullMQ 대안.** BullMQ는 Redis에 고정되어 있고 job semantics 자체가 Redis Lua 스크립트에 묶여
  있다. Taskrail은 broker-agnostic하며, memory adapter가 프로덕션과 **동일한 상태 머신**을 돌리므로
  Redis 컨테이너 없이 retry/timeout/DEAD 경로까지 단위 테스트로 검증할 수 있다.
- **Temporal의 대안이 아니라 반대편.** Temporal은 durable workflow engine(결정적 replay, 별도
  클러스터)이다. Taskrail은 **workflow engine이 아니다** — DAG도, 분기도, 보상 트랜잭션도 없다.
  job 하나 = handler 한 번 호출. 라이브러리지 서버가 아니다.
- **보장을 과장하지 않는다.** **at-least-once job execution**을 제공한다. 중복 실행은 일어날 수
  있고, `ctx.jobId`는 모든 attempt에서 동일하므로 idempotency key로 쓸 수 있다.

### 설치

```sh
npm install @taskrail/core @taskrail/queue-memory
```

> 아직 npm에 배포되지 않았다 — 첫 릴리스 이후를 기준으로 한 설치 명령이다.

### 최소 사용 예시

영문 [Quick start](#quick-start) 절의 예시 코드와 동일하다 — `taskrail.job()`으로 정의, `.enqueue()`로
요청, `taskrail.createWorker()`로 실행. 프로덕션 전환은 `MemoryQueue` → `DriftmqQueue` 한 줄 교체.

### MVP 범위

**포함:** job define/enqueue, worker pool, retry + backoff, timeout, concurrency, delayed job, job
state, graceful shutdown, `@taskrail/queue-memory`, `@taskrail/queue-driftmq`

**미포함(현재):** cron 기반 scheduled job, `@taskrail/queue-postgres`, Beacon 연동, CLI/대시보드,
DAG·workflow, priority queue, rate limit

### 기여

첫 릴리스 이후 issue/PR을 받는다. 아직 API 표면이 빠르게 바뀌는 단계라, 큰 PR을 보내기 전에 먼저
issue로 논의해주면 좋다.

### 라이선스

[MIT](LICENSE)
