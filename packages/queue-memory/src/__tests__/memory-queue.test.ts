import assert from "node:assert/strict";
import test from "node:test";

import { Taskrail } from "@taskrail/core";
import type { Delivery, JobEnvelope } from "@taskrail/core";
import {
  computeBackoffDelay,
  isJobEnvelope,
  nextAttemptEnvelope,
} from "@taskrail/core/internal";

import { MemoryQueue } from "../index.js";

const QUEUE = "default";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function envelope(overrides: Partial<JobEnvelope> = {}): JobEnvelope {
  const now = Date.now();
  return {
    v: 1,
    jobId: `J${Math.random().toString(36).slice(2)}`,
    name: "charge",
    queue: QUEUE,
    payload: { amount: 10 },
    attempt: 1,
    maxAttempts: 3,
    enqueuedAt: now,
    notBefore: now,
    timeoutMs: 30_000,
    backoff: { type: "fixed", delayMs: 10, jitter: "none" },
    ...overrides,
  };
}

/** 테스트용 미니 consumer. AbortController까지 묶어 정리를 한 줄로 만든다. */
async function consume(
  queue: MemoryQueue,
  handler: (delivery: Delivery) => Promise<void>,
  prefetch = 1,
): Promise<() => Promise<void>> {
  const controller = new AbortController();
  const subscription = await queue.consume(handler, {
    queue: QUEUE,
    prefetch,
    signal: controller.signal,
  });
  return async () => {
    controller.abort();
    await subscription.close();
  };
}

test("capabilities는 선언값과 실제 동작이 일치한다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 1_234 });

  assert.equal(queue.name, "memory");
  assert.deepEqual(queue.capabilities, {
    nativeDelay: true,
    redelivery: true,
    visibilityTimeoutMs: 1_234,
    nativeDeadLetter: false,
  });

  await queue.close();
});

test("enqueue → consume → ack: 한 번만 전달되고 ack 후 재전달되지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 60 });
  const seen: Delivery[] = [];
  const first = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    seen.push(delivery);
    await queue.ack(delivery.receipt);
    first.resolve();
  });

  const env = envelope();
  await queue.enqueue(env);
  await first.promise;

  assert.equal(seen.length, 1);
  assert.equal(seen[0]?.envelope.jobId, env.jobId);
  assert.equal(seen[0]?.deliveryCount, 1);
  assert.equal(seen[0]?.receipt.adapter, "memory");

  await sleep(200);
  assert.equal(seen.length, 1);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });

  await stop();
  await queue.close();
});

test("nack하면 같은 attempt로 재전달된다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const seen: Delivery[] = [];
  const redelivered = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    seen.push(delivery);
    if (seen.length === 1) {
      await queue.nack(delivery.receipt);
      return;
    }
    await queue.ack(delivery.receipt);
    redelivered.resolve();
  });

  const env = envelope();
  await queue.enqueue(env);
  await redelivered.promise;

  assert.equal(seen.length, 2);
  assert.equal(seen[1]?.envelope.jobId, env.jobId);
  assert.equal(seen[1]?.envelope.attempt, 1);
  assert.equal(seen[1]?.deliveryCount, 2);

  await stop();
  await queue.close();
});

test("ack하지 않으면 lease 만료 후 재전달된다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 60 });
  const counts: number[] = [];
  const redelivered = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    counts.push(delivery.deliveryCount);
    if (counts.length === 1) return;
    await queue.ack(delivery.receipt);
    redelivered.resolve();
  });

  await queue.enqueue(envelope());
  await redelivered.promise;

  assert.deepEqual(counts, [1, 2]);

  await stop();
  await queue.close();
});

test("notBefore 이전에는 전달하지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const notBefore = Date.now() + 120;
  const arrived = deferred<number>();

  const stop = await consume(queue, async (delivery) => {
    await queue.ack(delivery.receipt);
    arrived.resolve(Date.now());
  });

  await queue.enqueue(envelope({ notBefore }));
  assert.equal(queue.stats(QUEUE).ready, 1);

  const at = await arrived.promise;
  assert.ok(at >= notBefore, `전달 시각 ${at} < notBefore ${notBefore}`);

  await stop();
  await queue.close();
});

test("지연된 메시지가 먼저 들어와도 즉시 실행 가능한 메시지를 막지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const order: string[] = [];
  const both = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    order.push(delivery.envelope.jobId);
    await queue.ack(delivery.receipt);
    if (order.length === 2) both.resolve();
  });

  await queue.enqueue(envelope({ jobId: "late", notBefore: Date.now() + 80 }));
  await queue.enqueue(envelope({ jobId: "now" }));

  await both.promise;
  assert.deepEqual(order, ["now", "late"]);

  await stop();
  await queue.close();
});

test("prefetch를 넘겨 전달하지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const held: Delivery[] = [];
  const firstArrived = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    held.push(delivery);
    if (held.length === 1) firstArrived.resolve();
  }, 1);

  await queue.enqueue(envelope());
  await queue.enqueue(envelope());
  await queue.enqueue(envelope());
  await firstArrived.promise;

  await sleep(50);
  assert.equal(held.length, 1);
  assert.deepEqual(queue.stats(QUEUE), { ready: 2, inflight: 1 });

  const first = held[0];
  assert.ok(first !== undefined);
  await queue.ack(first.receipt);
  await sleep(50);
  assert.equal(held.length, 2);

  await stop();
  await queue.close();
});

test("deadLetter는 envelope과 사유를 함께 보관한다", async () => {
  const queue = new MemoryQueue();
  const env = envelope();

  await queue.deadLetter(env, {
    kind: "attempts-exhausted",
    attempt: 3,
    error: "upstream 503",
  });

  const records = queue.deadLetters();
  assert.equal(records.length, 1);
  assert.equal(records[0]?.envelope.jobId, env.jobId);
  assert.deepEqual(records[0]?.reason, {
    kind: "attempts-exhausted",
    attempt: 3,
    error: "upstream 503",
  });

  await queue.close();
});

test("nack(requeue: false)는 메시지를 버리지 않고 dead로 보낸다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const seen: Delivery[] = [];
  const nacked = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    seen.push(delivery);
    await queue.nack(delivery.receipt, { requeue: false });
    nacked.resolve();
  });

  const env = envelope();
  await queue.enqueue(env);
  await nacked.promise;
  await sleep(60);

  assert.equal(seen.length, 1);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });
  assert.equal(queue.deadLetters()[0]?.envelope.jobId, env.jobId);

  await stop();
  await queue.close();
});

test("lease 만료 뒤 도착한 늦은 ack은 새 전달을 확정하지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 60 });
  const seen: Delivery[] = [];
  const second = deferred<void>();

  const stop = await consume(queue, async (delivery) => {
    seen.push(delivery);
    if (seen.length === 2) second.resolve();
  });

  await queue.enqueue(envelope());
  await second.promise;

  const stale = seen[0];
  assert.ok(stale !== undefined);
  await queue.ack(stale.receipt);

  assert.equal(queue.stats(QUEUE).inflight, 1);

  await stop();
  await queue.close();
});

test("다른 어댑터의 receipt는 거부한다", async () => {
  const queue = new MemoryQueue();
  await assert.rejects(
    () => queue.ack({ adapter: "driftmq", token: 42 }),
    /driftmq/,
  );
  await queue.close();
});

test("maxQueueSize를 넘기면 enqueue가 throw한다", async () => {
  const queue = new MemoryQueue({ maxQueueSize: 2 });
  await queue.enqueue(envelope());
  await queue.enqueue(envelope());
  await assert.rejects(() => queue.enqueue(envelope()), /maxQueueSize/);
  await queue.close();
});

test("envelope이 아닌 값은 enqueue되지 않는다", async () => {
  const queue = new MemoryQueue();
  assert.equal(isJobEnvelope({ hello: "world" }), false);
  await assert.rejects(
    () => queue.enqueue({ hello: "world" } as unknown as JobEnvelope),
    /JobEnvelope/,
  );
  await queue.close();
});

test("close 후에는 enqueue와 consume이 거부된다", async () => {
  const queue = new MemoryQueue();
  await queue.close();

  await assert.rejects(() => queue.enqueue(envelope()), /close/);
  await assert.rejects(
    () =>
      queue.consume(async () => {}, {
        queue: QUEUE,
        prefetch: 1,
        signal: new AbortController().signal,
      }),
    /close/,
  );
});

test("signal을 abort하면 더 이상 전달받지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const seen: Delivery[] = [];
  const controller = new AbortController();

  await queue.consume(
    async (delivery) => {
      seen.push(delivery);
      await queue.ack(delivery.receipt);
    },
    { queue: QUEUE, prefetch: 1, signal: controller.signal },
  );

  controller.abort();
  await queue.enqueue(envelope());
  await sleep(50);

  assert.equal(seen.length, 0);
  assert.equal(queue.stats(QUEUE).ready, 1);

  await queue.close();
});

test("core 경계면: job 하나를 enqueue → 실행 성공 → ack", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 100 });
  const taskrail = new Taskrail({ queue });
  const executed: Array<{ jobId: string; attempt: number; deliveryCount: number }> = [];
  const done = deferred<void>();

  const charge = taskrail.job("charge", async () => {});
  const { jobId } = await charge.enqueue({ orderId: "o-1" });

  const stop = await consume(queue, async (delivery) => {
    executed.push({
      jobId: delivery.envelope.jobId,
      attempt: delivery.envelope.attempt,
      deliveryCount: delivery.deliveryCount,
    });
    await queue.ack(delivery.receipt);
    done.resolve();
  });

  await done.promise;
  assert.deepEqual(executed, [{ jobId, attempt: 1, deliveryCount: 1 }]);
  assert.equal((await taskrail.getState(jobId))?.status, "WAITING");

  // lease보다 길게 기다려도 재전달이 없어야 ack이 실제로 먹힌 것이다.
  await sleep(250);
  assert.equal(executed.length, 1);

  await stop();
  await taskrail.close();
});

test("core 경계면: 실패 → 백오프 재enqueue → 원본 ack → 다음 attempt 실행", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const attempts: number[] = [];
  const done = deferred<void>();
  let retryAt = 0;
  let ranAt = 0;

  const charge = taskrail.job("charge", async () => {}, {
    maxAttempts: 3,
    backoff: { type: "fixed", delayMs: 80, jitter: "none" },
  });
  const { jobId } = await charge.enqueue({ orderId: "o-2" });

  const stop = await consume(queue, async (delivery) => {
    const env = delivery.envelope;
    attempts.push(env.attempt);
    assert.equal(env.jobId, jobId);

    if (env.attempt === 1) {
      const error = new Error("upstream 503");
      const delayMs = computeBackoffDelay(env.backoff, env.attempt, error);
      retryAt = Date.now() + delayMs;
      await queue.enqueue(nextAttemptEnvelope(env, retryAt));
      await queue.ack(delivery.receipt);
      return;
    }

    ranAt = Date.now();
    await queue.ack(delivery.receipt);
    done.resolve();
  });

  await done.promise;

  assert.deepEqual(attempts, [1, 2]);
  assert.ok(ranAt >= retryAt, `재시도 ${ranAt} < 백오프 만료 ${retryAt}`);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });
  assert.equal(queue.deadLetters().length, 0);

  await stop();
  await taskrail.close();
});

test("core 경계면: 재시도 소진은 dead로 끝난다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const attempts: number[] = [];
  const done = deferred<void>();

  const charge = taskrail.job("charge", async () => {}, {
    maxAttempts: 2,
    backoff: { type: "fixed", delayMs: 10, jitter: "none" },
  });
  const { jobId } = await charge.enqueue({ orderId: "o-3" });

  const stop = await consume(queue, async (delivery) => {
    const env = delivery.envelope;
    attempts.push(env.attempt);

    if (env.attempt < env.maxAttempts) {
      await queue.enqueue(nextAttemptEnvelope(env, Date.now() + 10));
      await queue.ack(delivery.receipt);
      return;
    }

    await queue.deadLetter(env, {
      kind: "attempts-exhausted",
      attempt: env.attempt,
      error: "upstream 503",
    });
    await queue.ack(delivery.receipt);
    done.resolve();
  });

  await done.promise;

  assert.deepEqual(attempts, [1, 2]);
  const record = queue.deadLetters()[0];
  assert.equal(record?.envelope.jobId, jobId);
  assert.equal(record?.reason.kind, "attempts-exhausted");
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });

  await stop();
  await taskrail.close();
});
