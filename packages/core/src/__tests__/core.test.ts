import assert from "node:assert/strict";
import test from "node:test";

import {
  FatalJobError,
  MemoryStateStore,
  Taskrail,
  TimeoutError,
  WIRE_HEADERS,
  toWireHeaders,
} from "../index.js";
import type {
  Delivery,
  DeliveryReceipt,
  JobEnvelope,
  JobQueue,
  QueueCapabilities,
  Subscription,
} from "../index.js";
import {
  canTransition,
  computeBackoffDelay,
  isJobEnvelope,
  nextAttemptEnvelope,
  resolveWorkerOptions,
} from "../internal.js";

/** 계약 검증용 최소 큐. 진짜 구현체는 2단계(@taskrail/queue-memory)다. */
class FakeQueue implements JobQueue {
  readonly name = "fake";
  readonly capabilities: QueueCapabilities = {
    nativeDelay: true,
    redelivery: true,
    visibilityTimeoutMs: 30_000,
    nativeDeadLetter: false,
  };
  readonly enqueued: JobEnvelope[] = [];
  closed = false;

  async enqueue(envelope: JobEnvelope): Promise<void> {
    this.enqueued.push(envelope);
  }

  async consume(
    _handler: (delivery: Delivery) => Promise<void>,
    _options: { queue: string; prefetch: number; signal: AbortSignal },
  ): Promise<Subscription> {
    return { close: async () => {} };
  }

  async ack(_receipt: DeliveryReceipt): Promise<void> {}
  async nack(_receipt: DeliveryReceipt, _options?: { requeue?: boolean }): Promise<void> {}
  async deadLetter(_envelope: JobEnvelope, _reason: unknown): Promise<void> {}
  async close(): Promise<void> {
    this.closed = true;
  }
}

test("job()은 payload 타입을 추론하고 enqueue 가능한 정의를 돌려준다", async () => {
  const queue = new FakeQueue();
  const taskrail = new Taskrail({ queue });

  const sendEmail = taskrail.job(
    "send-email",
    async (payload: { userId: string }, ctx) => {
      assert.equal(typeof payload.userId, "string");
      assert.equal(typeof ctx.jobId, "string");
    },
  );

  assert.equal(sendEmail.name, "send-email");

  const handle = await sendEmail.enqueue({ userId: "1234" });
  assert.equal(queue.enqueued.length, 1);

  const envelope = queue.enqueued[0]!;
  assert.equal(envelope.v, 1);
  assert.equal(envelope.jobId, handle.jobId);
  assert.equal(envelope.name, "send-email");
  assert.equal(envelope.queue, "default");
  assert.equal(envelope.attempt, 1);
  assert.equal(envelope.maxAttempts, 3);
  assert.equal(envelope.timeoutMs, 30_000);
  assert.deepEqual(envelope.payload, { userId: "1234" });
  assert.ok(isJobEnvelope(envelope));
});

test("같은 이름을 두 번 정의하면 throw", () => {
  const taskrail = new Taskrail({ queue: new FakeQueue() });
  taskrail.job("dup", async () => {});
  assert.throws(() => taskrail.job("dup", async () => {}), /이미 정의/);
});

test("옵션 우선순위: enqueue > job 정의 > Taskrail defaults", async () => {
  const queue = new FakeQueue();
  const taskrail = new Taskrail({
    queue,
    defaults: { maxAttempts: 2, timeoutMs: 1_000 },
  });
  const job = taskrail.job("opts", async () => {}, { maxAttempts: 5 });

  await job.enqueue(undefined, { timeoutMs: 7_000 });
  const envelope = queue.enqueued[0]!;
  assert.equal(envelope.maxAttempts, 5);
  assert.equal(envelope.timeoutMs, 7_000);
});

test("delayMs와 runAt을 동시에 주면 throw", async () => {
  const taskrail = new Taskrail({ queue: new FakeQueue() });
  const job = taskrail.job("delayed", async () => {});
  await assert.rejects(
    () => job.enqueue(undefined, { delayMs: 10, runAt: new Date() }),
    /동시에/,
  );
});

test("delayMs는 notBefore로 정규화된다", async () => {
  const queue = new FakeQueue();
  const taskrail = new Taskrail({ queue });
  const job = taskrail.job("later", async () => {});

  const before = Date.now();
  await job.enqueue(undefined, { delayMs: 60_000 });
  assert.ok(queue.enqueued[0]!.notBefore >= before + 60_000);
});

test("custom backoff는 직렬화 불가라 enqueue에서 거부된다", async () => {
  const taskrail = new Taskrail({ queue: new FakeQueue() });
  const job = taskrail.job("custom-backoff", async () => {}, {
    backoff: { type: "custom", compute: () => 1_000 },
  });
  await assert.rejects(() => job.enqueue(undefined), /직렬화/);
});

test("JSON 직렬화 불가 payload는 큐에 들어가지 않는다", async () => {
  const queue = new FakeQueue();
  const taskrail = new Taskrail({ queue });
  const job = taskrail.job("poison", async (_p: unknown) => {});

  const circular: Record<string, unknown> = {};
  circular["self"] = circular;

  await assert.rejects(() => job.enqueue(circular), /직렬화/);
  assert.equal(queue.enqueued.length, 0);
});

test("job.enqueued 이벤트와 getState(WAITING)", async () => {
  const taskrail = new Taskrail({ queue: new FakeQueue() });
  const seen: string[] = [];
  const listener = (e: { jobId: string }) => seen.push(e.jobId);
  taskrail.on("job.enqueued", listener);

  const job = taskrail.job("observed", async () => {});
  const { jobId } = await job.enqueue(undefined);

  assert.deepEqual(seen, [jobId]);

  const snapshot = await taskrail.getState(jobId);
  assert.equal(snapshot?.status, "WAITING");
  assert.equal(snapshot?.attempt, 1);

  taskrail.off("job.enqueued", listener);
  await job.enqueue(undefined);
  assert.equal(seen.length, 1);
});

test("이벤트 리스너가 던져도 enqueue는 영향받지 않는다", async () => {
  const taskrail = new Taskrail({ queue: new FakeQueue() });
  taskrail.on("job.enqueued", () => {
    throw new Error("listener boom");
  });
  const job = taskrail.job("resilient", async () => {});
  const handle = await job.enqueue(undefined);
  assert.equal(typeof handle.jobId, "string");
});

test("close() 후에는 enqueue가 거부된다", async () => {
  const queue = new FakeQueue();
  const taskrail = new Taskrail({ queue });
  const job = taskrail.job("closing", async () => {});
  await taskrail.close();
  assert.equal(queue.closed, true);
  await assert.rejects(() => job.enqueue(undefined), /close/);
});

test("backoff: exponential base 수열 (jitter none)", () => {
  const policy = {
    type: "exponential",
    delayMs: 1_000,
    factor: 2,
    maxDelayMs: 60_000,
    jitter: "none",
  } as const;
  const got = [1, 2, 3, 4, 5, 6, 7].map((attempt) =>
    computeBackoffDelay(policy, attempt, new Error("x")),
  );
  assert.deepEqual(got, [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000]);
});

test("backoff: full jitter는 base에 비례해 줄어든다", () => {
  const delay = computeBackoffDelay(
    { type: "fixed", delayMs: 1_000, jitter: "full" },
    1,
    undefined,
    () => 0.5,
  );
  assert.equal(delay, 500);
});

test("상태 머신 전이 규칙", () => {
  assert.ok(canTransition("WAITING", "RUNNING"));
  assert.ok(canTransition("RUNNING", "FAILED"));
  assert.ok(canTransition("FAILED", "RETRY_WAIT"));
  assert.ok(canTransition("FAILED", "DEAD"));
  assert.ok(canTransition("RETRY_WAIT", "WAITING"));
  assert.ok(canTransition("RUNNING", "WAITING"));
  assert.ok(canTransition("WAITING", "DEAD"));
  assert.equal(canTransition("SUCCESS", "RUNNING"), false);
  assert.equal(canTransition("DEAD", "WAITING"), false);
  assert.equal(canTransition("RUNNING", "RUNNING"), false);
});

test("재시도 envelope은 attempt만 올리고 jobId/enqueuedAt은 보존한다", () => {
  const original: JobEnvelope = {
    v: 1,
    jobId: "J1",
    name: "n",
    queue: "default",
    payload: { a: 1 },
    attempt: 1,
    maxAttempts: 3,
    enqueuedAt: 1_000,
    notBefore: 1_000,
    timeoutMs: 30_000,
    backoff: { type: "fixed", delayMs: 100 },
  };
  const next = nextAttemptEnvelope(original, 5_000);
  assert.equal(next.attempt, 2);
  assert.equal(next.jobId, "J1");
  assert.equal(next.enqueuedAt, 1_000);
  assert.equal(next.notBefore, 5_000);
});

test("wire 헤더는 x-driftmq- 접두사를 쓰지 않는다", () => {
  const headers = toWireHeaders({
    v: 1,
    jobId: "J1",
    name: "charge",
    queue: "default",
    payload: null,
    attempt: 2,
    maxAttempts: 3,
    enqueuedAt: 1,
    notBefore: 2,
    timeoutMs: 30_000,
    backoff: { type: "fixed", delayMs: 100 },
  });
  assert.equal(headers[WIRE_HEADERS.version], "1");
  assert.equal(headers[WIRE_HEADERS.jobId], "J1");
  assert.equal(headers[WIRE_HEADERS.attempt], "2");
  for (const key of Object.keys(headers)) {
    assert.equal(key.startsWith("x-driftmq-"), false);
  }
});

test("worker 옵션 기본값: prefetch = concurrency", () => {
  assert.deepEqual(resolveWorkerOptions(undefined), {
    queues: ["default"],
    concurrency: 10,
    prefetch: 10,
    inlineDelayMs: 5_000,
    requeueIntervalMs: 30_000,
  });
  assert.equal(resolveWorkerOptions({ concurrency: 20 }).prefetch, 20);
});

test("createWorker는 Worker 표면을 만족한다 (실행 루프는 3단계)", async () => {
  const taskrail = new Taskrail({ queue: new FakeQueue() });
  const worker = taskrail.createWorker({ concurrency: 2 });
  assert.equal(typeof worker.id, "string");
  assert.equal(worker.inFlight, 0);
  await assert.rejects(() => worker.start(), /3단계/);
  await worker.stop();
  await worker.stop();
});

test("에러 타입", () => {
  assert.ok(new FatalJobError("x") instanceof Error);
  assert.equal(new TimeoutError(1_000).timeoutMs, 1_000);
});

test("MemoryStateStore는 프로세스 로컬 저장소다", async () => {
  const store = new MemoryStateStore();
  assert.equal(await store.get("nope"), undefined);
  await store.record({
    jobId: "J1",
    name: "n",
    queue: "default",
    status: "SUCCESS",
    attempt: 1,
    maxAttempts: 3,
    enqueuedAt: new Date(0),
  });
  assert.equal((await store.get("J1"))?.status, "SUCCESS");
});
