import assert from "node:assert/strict";
import test from "node:test";

import { FatalJobError, Taskrail } from "@taskrail/core";
import type { JobContext, JobEnvelope, TaskrailEvents, Worker } from "@taskrail/core";

import { MemoryQueue } from "../index.js";

const QUEUE = "default";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function deferred<T = void>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/** 이벤트 이름 순서를 그대로 모아 두면 전이 순서를 한 줄로 검증할 수 있다. */
function trace(taskrail: Taskrail): string[] {
  const seen: string[] = [];
  const names = [
    "job.enqueued",
    "job.started",
    "job.completed",
    "job.failed",
    "job.retried",
    "job.dead",
  ] as const;
  for (const name of names) {
    taskrail.on(name, (() => seen.push(name)) as TaskrailEvents[typeof name]);
  }
  return seen;
}

async function shutdown(worker: Worker, taskrail: Taskrail): Promise<void> {
  await worker.stop({ timeoutMs: 1_000 });
  await taskrail.close();
}

test("성공 경로: 실행 → ack → SUCCESS", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const events = trace(taskrail);
  const done = deferred();
  const payloads: unknown[] = [];

  const charge = taskrail.job("charge", async (payload: { orderId: string }, ctx) => {
    payloads.push(payload);
    assert.equal(ctx.attempt, 1);
    assert.equal(ctx.deliveryCount, 1);
    assert.equal(ctx.signal.aborted, false);
  });
  taskrail.on("job.completed", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 2 });
  await worker.start();

  const { jobId } = await charge.enqueue({ orderId: "o-1" });
  await done.promise;

  assert.deepEqual(payloads, [{ orderId: "o-1" }]);
  assert.deepEqual(events, ["job.enqueued", "job.started", "job.completed"]);

  const snapshot = await taskrail.getState(jobId);
  assert.equal(snapshot?.status, "SUCCESS");
  assert.equal(snapshot?.attempt, 1);
  assert.ok(snapshot?.startedAt instanceof Date);
  assert.ok(snapshot?.finishedAt instanceof Date);

  await sleep(50);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });
  assert.equal(queue.deadLetters().length, 0);
  assert.equal(worker.inFlight, 0);

  await shutdown(worker, taskrail);
});

test("재시도 후 성공: attempt가 오르고 백오프만큼 기다린다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const events = trace(taskrail);
  const done = deferred();
  const attempts: number[] = [];
  const retried: Array<{ nextAttempt: number; delayMs: number }> = [];
  let failedAt = 0;
  let retriedRanAt = 0;

  const charge = taskrail.job(
    "charge",
    async (_payload: unknown, ctx: JobContext) => {
      attempts.push(ctx.attempt);
      if (ctx.attempt === 1) {
        failedAt = Date.now();
        throw new Error("upstream 503");
      }
      retriedRanAt = Date.now();
    },
    { maxAttempts: 3, backoff: { type: "fixed", delayMs: 80, jitter: "none" } },
  );
  taskrail.on("job.retried", (e) =>
    retried.push({ nextAttempt: e.nextAttempt, delayMs: e.delayMs }),
  );
  taskrail.on("job.completed", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();

  const { jobId } = await charge.enqueue({ orderId: "o-2" });
  await done.promise;

  assert.deepEqual(attempts, [1, 2]);
  assert.deepEqual(retried, [{ nextAttempt: 2, delayMs: 80 }]);
  assert.ok(
    retriedRanAt - failedAt >= 80,
    `백오프를 지키지 않았다: ${retriedRanAt - failedAt}ms`,
  );
  assert.deepEqual(events, [
    "job.enqueued",
    "job.started",
    "job.failed",
    "job.retried",
    "job.started",
    "job.completed",
  ]);

  const snapshot = await taskrail.getState(jobId);
  assert.equal(snapshot?.status, "SUCCESS");
  assert.equal(snapshot?.attempt, 2);
  assert.equal(snapshot?.lastError?.message, "upstream 503");
  assert.equal(queue.deadLetters().length, 0);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });

  await shutdown(worker, taskrail);
});

test("재시도 소진: attempts-exhausted로 DEAD", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const events = trace(taskrail);
  const done = deferred();
  const attempts: number[] = [];

  const charge = taskrail.job(
    "charge",
    async (_payload: unknown, ctx: JobContext) => {
      attempts.push(ctx.attempt);
      throw new Error("upstream 503");
    },
    { maxAttempts: 2, backoff: { type: "fixed", delayMs: 10, jitter: "none" } },
  );
  taskrail.on("job.dead", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();

  const { jobId } = await charge.enqueue({ orderId: "o-3" });
  await done.promise;

  assert.deepEqual(attempts, [1, 2]);
  assert.deepEqual(events, [
    "job.enqueued",
    "job.started",
    "job.failed",
    "job.retried",
    "job.started",
    "job.failed",
    "job.dead",
  ]);

  const record = queue.deadLetters()[0];
  assert.equal(queue.deadLetters().length, 1);
  assert.equal(record?.envelope.jobId, jobId);
  assert.deepEqual(record?.reason, {
    kind: "attempts-exhausted",
    attempt: 2,
    error: "upstream 503",
  });

  const snapshot = await taskrail.getState(jobId);
  assert.equal(snapshot?.status, "DEAD");
  assert.equal(snapshot?.attempt, 2);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });

  await shutdown(worker, taskrail);
});

test("FatalJobError는 재시도 예산을 쓰지 않고 즉시 DEAD", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const done = deferred();
  let runs = 0;

  const charge = taskrail.job(
    "charge",
    async () => {
      runs += 1;
      throw new FatalJobError("userId 누락");
    },
    { maxAttempts: 5 },
  );
  taskrail.on("job.dead", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();

  const { jobId } = await charge.enqueue({});
  await done.promise;
  await sleep(50);

  assert.equal(runs, 1);
  assert.deepEqual(queue.deadLetters()[0]?.reason, {
    kind: "fatal",
    error: "userId 누락",
  });
  assert.equal((await taskrail.getState(jobId))?.status, "DEAD");

  await shutdown(worker, taskrail);
});

test("timeout: 슬롯을 회수하고 TimeoutError로 실패 처리한다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const done = deferred();
  const release = deferred();
  const failures: Array<{ timedOut: boolean; message: string }> = [];
  let aborted = false;

  const slow = taskrail.job(
    "slow",
    async (_payload: unknown, ctx: JobContext) => {
      ctx.signal.addEventListener("abort", () => {
        aborted = true;
      });
      await release.promise;
    },
    { maxAttempts: 1, timeoutMs: 60 },
  );
  taskrail.on("job.failed", (e) => {
    failures.push({
      timedOut: e.timedOut,
      message: (e.error as Error).message,
    });
  });
  taskrail.on("job.dead", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();

  const { jobId } = await slow.enqueue({});
  await done.promise;
  await sleep(20);

  assert.equal(failures.length, 1);
  assert.equal(failures[0]?.timedOut, true);
  assert.match(failures[0]?.message ?? "", /timed out after 60ms/);
  assert.equal(aborted, true, "handler의 ctx.signal이 abort되어야 한다");
  // handler는 아직 release를 기다리는 중이다 — 그래도 슬롯은 이미 회수되어 있어야 한다.
  assert.equal(worker.inFlight, 0, "타임아웃 후 동시성 슬롯이 회수되어야 한다");
  assert.equal((await taskrail.getState(jobId))?.status, "DEAD");
  assert.equal(queue.deadLetters()[0]?.reason.kind, "attempts-exhausted");

  release.resolve();
  await shutdown(worker, taskrail);
});

test("concurrency: prefetch가 더 커도 동시에 실행되는 handler 수를 넘지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const total = 8;
  const done = deferred();
  let running = 0;
  let peak = 0;
  let finished = 0;

  const work = taskrail.job("work", async () => {
    running += 1;
    peak = Math.max(peak, running);
    await sleep(20);
    running -= 1;
  });
  taskrail.on("job.completed", () => {
    finished += 1;
    if (finished === total) done.resolve();
  });

  const worker = taskrail.createWorker({ concurrency: 2, prefetch: total });
  await worker.start();

  for (let i = 0; i < total; i += 1) await work.enqueue({ i });
  await done.promise;

  assert.equal(finished, total);
  assert.equal(peak, 2, `동시 실행 상한 위반: peak=${peak}`);
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });

  await shutdown(worker, taskrail);
});

test("graceful shutdown: 진행 중인 job은 끝내고, 시작 안 한 전달은 큐로 돌려준다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const started = deferred();
  const release = deferred();
  const completed: string[] = [];
  let sawAbort = false;

  const work = taskrail.job("work", async (_payload: unknown, ctx: JobContext) => {
    started.resolve();
    await release.promise;
    sawAbort = ctx.signal.aborted;
  });
  taskrail.on("job.completed", (e) => completed.push(e.jobId));

  const worker = taskrail.createWorker({ concurrency: 1, prefetch: 3 });
  await worker.start();

  const first = await work.enqueue({ n: 1 });
  await work.enqueue({ n: 2 });
  await work.enqueue({ n: 3 });
  await started.promise;

  const stopping = worker.stop({ timeoutMs: 1_000 });
  await sleep(20);
  release.resolve();
  await stopping;

  assert.deepEqual(completed, [first.jobId], "진행 중이던 job만 완료되어야 한다");
  assert.equal(sawAbort, true, "shutdown이 ctx.signal로 전달되어야 한다");
  assert.equal((await taskrail.getState(first.jobId))?.status, "SUCCESS");
  assert.deepEqual(
    queue.stats(QUEUE),
    { ready: 2, inflight: 0 },
    "시작하지 않은 전달은 nack(requeue)되어야 한다",
  );
  assert.equal(queue.deadLetters().length, 0);
  assert.equal(worker.inFlight, 0);

  await taskrail.close();
});

test("graceful shutdown: 기한을 넘긴 job은 포기하고 반환한다 (프로세스를 막지 않는다)", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const started = deferred();
  const release = deferred();

  const work = taskrail.job("work", async () => {
    started.resolve();
    await release.promise;
  });

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();
  await work.enqueue({});
  await started.promise;

  const at = Date.now();
  await worker.stop({ timeoutMs: 60 });
  const elapsed = Date.now() - at;

  assert.ok(elapsed >= 60 && elapsed < 1_000, `stop()이 기한을 지키지 않았다: ${elapsed}ms`);
  assert.equal(worker.inFlight, 1, "포기한 job은 여전히 실행 중이다");
  // ack하지 않았으므로 브로커 lease가 만료되면 재전달된다.
  assert.equal(queue.stats(QUEUE).inflight, 1);

  release.resolve();
  await taskrail.close();
});

test("알 수 없는 job 이름은 실행 없이 DEAD + ack", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const done = deferred();
  const dead: string[] = [];

  taskrail.job("known", async () => {});
  taskrail.on("job.dead", (e) => {
    dead.push(e.reason.kind);
    done.resolve();
  });
  taskrail.on("job.failed", () => assert.fail("실행하지 않았으므로 job.failed는 없다"));

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();

  const now = Date.now();
  const orphan: JobEnvelope = {
    v: 1,
    jobId: "J-orphan",
    name: "removed-in-old-deploy",
    queue: QUEUE,
    payload: null,
    attempt: 1,
    maxAttempts: 3,
    enqueuedAt: now,
    notBefore: now,
    timeoutMs: 30_000,
    backoff: { type: "fixed", delayMs: 10 },
  };
  await queue.enqueue(orphan);
  await done.promise;

  assert.deepEqual(dead, ["unknown-job"]);
  assert.deepEqual(queue.deadLetters()[0]?.reason, {
    kind: "unknown-job",
    name: "removed-in-old-deploy",
  });
  assert.equal((await taskrail.getState("J-orphan"))?.status, "DEAD");
  assert.deepEqual(queue.stats(QUEUE), { ready: 0, inflight: 0 });

  await shutdown(worker, taskrail);
});

test("delayed job: notBefore 이전에는 실행되지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 5_000 });
  const taskrail = new Taskrail({ queue });
  const done = deferred();
  let ranAt = 0;

  const later = taskrail.job("later", async () => {
    ranAt = Date.now();
  });
  taskrail.on("job.completed", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();

  const notBefore = Date.now() + 120;
  await later.enqueue({}, { delayMs: 120 });
  await done.promise;

  assert.ok(ranAt >= notBefore, `실행 시각 ${ranAt} < notBefore ${notBefore}`);

  await shutdown(worker, taskrail);
});

test("lease 만료 재전달은 attempt를 소모하지 않는다", async () => {
  const queue = new MemoryQueue({ visibilityTimeoutMs: 60 });
  const taskrail = new Taskrail({ queue });
  const done = deferred();
  const seen: Array<{ attempt: number; deliveryCount: number }> = [];
  const release = deferred();

  const work = taskrail.job(
    "work",
    async (_payload: unknown, ctx: JobContext) => {
      seen.push({ attempt: ctx.attempt, deliveryCount: ctx.deliveryCount });
      if (seen.length === 1) {
        await release.promise;
        return;
      }
      done.resolve();
    },
    { timeoutMs: 5_000 },
  );

  const worker = taskrail.createWorker({ concurrency: 2 });
  await worker.start();

  await work.enqueue({});
  await done.promise;

  assert.deepEqual(seen, [
    { attempt: 1, deliveryCount: 1 },
    { attempt: 1, deliveryCount: 2 },
  ]);
  assert.equal(queue.deadLetters().length, 0);

  release.resolve();
  await shutdown(worker, taskrail);
});
