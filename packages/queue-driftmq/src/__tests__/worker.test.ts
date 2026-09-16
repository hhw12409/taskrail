import assert from "node:assert/strict";
import test from "node:test";
import type { TestContext } from "node:test";

import { FatalJobError, Taskrail } from "@taskrail/core";

import { DriftmqQueue } from "../index.js";
import { FakeBroker } from "./support/fake-broker.js";

const TOPIC = "taskrail.default";
const DEAD_TOPIC = "taskrail.default.dead";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function deferred<T = void>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function deadReasons(broker: FakeBroker): string[] {
  return broker
    .records(DEAD_TOPIC)
    .map(
      (record) =>
        record.headers.find((header) => header.key === "dead-reason")?.value.toString("utf8") ?? "",
    );
}

/** core 런타임을 실제 소켓 위에서 돌린다 — 어댑터가 worker와 맞물리는지까지 본다. */
async function runtime(t: TestContext): Promise<{ broker: FakeBroker; taskrail: Taskrail }> {
  const broker = await FakeBroker.start();
  const queue = new DriftmqQueue({
    host: "127.0.0.1",
    port: broker.port,
    pollIntervalMs: 20,
    connectTimeoutMs: 1_000,
    requestTimeoutMs: 2_000,
  });
  const taskrail = new Taskrail({ queue });

  t.after(async () => {
    await taskrail.close();
    await broker.close();
  });

  return { broker, taskrail };
}

test("성공 경로: 실행 → ack → SUCCESS", async (t) => {
  const { broker, taskrail } = await runtime(t);
  const done = deferred();
  const payloads: unknown[] = [];

  const charge = taskrail.job("charge", async (payload: { orderId: string }, ctx) => {
    payloads.push(payload);
    assert.equal(ctx.attempt, 1);
    assert.equal(ctx.deliveryCount, 1);
  });
  taskrail.on("job.completed", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 2 });
  await worker.start();

  const { jobId } = await charge.enqueue({ orderId: "o-1" });
  await done.promise;

  assert.deepEqual(payloads, [{ orderId: "o-1" }]);
  assert.equal((await taskrail.getState(jobId))?.status, "SUCCESS");

  await sleep(150);
  assert.equal(broker.committed(TOPIC, "taskrail-default"), 1n, "ack이 브로커에 반영된다");
  assert.equal(broker.records(DEAD_TOPIC).length, 0);

  await worker.stop({ timeoutMs: 1_000 });
});

test("재시도: 실패하면 다음 attempt를 새 메시지로 publish하고 원본을 ack한다", async (t) => {
  const { broker, taskrail } = await runtime(t);
  const done = deferred();
  const attempts: number[] = [];

  const charge = taskrail.job(
    "charge",
    async (_payload: unknown, ctx) => {
      attempts.push(ctx.attempt);
      if (ctx.attempt === 1) throw new Error("upstream 503");
    },
    { maxAttempts: 3, backoff: { type: "fixed", delayMs: 20, jitter: "none" } },
  );
  taskrail.on("job.completed", () => done.resolve());

  const worker = taskrail.createWorker({ concurrency: 1, inlineDelayMs: 5_000 });
  await worker.start();

  await charge.enqueue({ orderId: "o-2" });
  await done.promise;

  assert.deepEqual(attempts, [1, 2]);
  assert.equal(broker.records(TOPIC).length, 2, "재시도는 새 메시지 한 통이다");
  assert.equal(broker.records(DEAD_TOPIC).length, 0);

  await sleep(150);
  assert.equal(broker.committed(TOPIC, "taskrail-default"), 2n, "원본과 재시도 모두 ack된다");

  await worker.stop({ timeoutMs: 1_000 });
});

test("재시도 소진과 FatalJobError는 dead 토픽으로 끝난다", async (t) => {
  const { broker, taskrail } = await runtime(t);
  const dead = deferred<string>();

  const exhausted = taskrail.job(
    "exhausted",
    async () => {
      throw new Error("upstream 503");
    },
    { maxAttempts: 2, backoff: { type: "fixed", delayMs: 10, jitter: "none" } },
  );
  const fatal = taskrail.job("fatal", async () => {
    throw new FatalJobError("amount must be positive");
  });

  const seen: string[] = [];
  taskrail.on("job.dead", (event) => {
    seen.push(event.name);
    if (seen.length === 2) dead.resolve(event.name);
  });

  const worker = taskrail.createWorker({ concurrency: 2 });
  await worker.start();

  await exhausted.enqueue({});
  await fatal.enqueue({});
  await dead.promise;
  await sleep(150);

  assert.deepEqual(deadReasons(broker).sort(), ["attempts-exhausted", "fatal"]);
  assert.equal(broker.committed(TOPIC, "taskrail-default"), 3n, "dead로 보낸 메시지도 ack한다");

  await worker.stop({ timeoutMs: 1_000 });
});

test("알 수 없는 job 이름은 실행 없이 dead로 간다", async (t) => {
  const { broker, taskrail } = await runtime(t);
  const dead = deferred();

  const known = taskrail.job("known", async () => {});
  await known.enqueue({});

  // worker가 등록하지 않은 이름으로 온 메시지를 흉내낸다.
  broker.createTopic(TOPIC);
  const [record] = broker.records(TOPIC);
  assert.ok(record !== undefined);
  const envelope = { ...JSON.parse(record.payload.toString("utf8")), name: "gone" };
  broker.append(TOPIC, [], Buffer.from(JSON.stringify(envelope), "utf8"));

  taskrail.on("job.dead", () => dead.resolve());

  const worker = taskrail.createWorker({ concurrency: 1 });
  await worker.start();
  await dead.promise;
  await sleep(150);

  assert.deepEqual(deadReasons(broker), ["unknown-job"]);

  await worker.stop({ timeoutMs: 1_000 });
});

test("nativeDelay가 없으므로 긴 지연은 재enqueue로 보완한다", async (t) => {
  const { broker, taskrail } = await runtime(t);
  const done = deferred();
  let ranAt = 0;

  const later = taskrail.job("later", async () => {
    ranAt = Date.now();
  });
  taskrail.on("job.completed", () => done.resolve());

  // inlineDelayMs보다 긴 지연은 슬롯을 점유하지 않도록 큐로 되돌린다.
  const worker = taskrail.createWorker({
    concurrency: 1,
    inlineDelayMs: 30,
    requeueIntervalMs: 40,
  });
  await worker.start();

  const runAfter = Date.now() + 200;
  await later.enqueue({}, { delayMs: 200 });
  await done.promise;

  assert.ok(ranAt >= runAfter, `실행 시각 ${ranAt} < notBefore ${runAfter}`);
  assert.ok(broker.records(TOPIC).length > 1, "지연 동안 같은 envelope이 다시 큐에 들어간다");
  assert.equal(broker.records(DEAD_TOPIC).length, 0);

  await worker.stop({ timeoutMs: 1_000 });
});
