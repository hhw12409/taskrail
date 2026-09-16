import assert from "node:assert/strict";
import test from "node:test";
import type { TestContext } from "node:test";

import type { Delivery, JobEnvelope } from "@taskrail/core";

import { DriftmqQueue } from "../index.js";
import { ErrorCode } from "../wire/protocol.js";
import { FakeBroker } from "./support/fake-broker.js";

const QUEUE = "default";
const TOPIC = "taskrail.default";
const DEAD_TOPIC = "taskrail.default.dead";
const CONSUMER = "taskrail-default";

function envelope(overrides: Partial<JobEnvelope> = {}): JobEnvelope {
  const now = 1_700_000_000_000;
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, what: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(10);
  }
  assert.fail(`${what} — ${timeoutMs}ms 안에 일어나지 않았다`);
}

interface Harness {
  readonly broker: FakeBroker;
  readonly queue: DriftmqQueue;
  consume(handler: (delivery: Delivery) => Promise<void>, prefetch?: number): Promise<void>;
}

/** 정리를 테스트 훅에 건다 — 실패해도 소켓/서버가 남지 않는다. */
async function harness(
  t: TestContext,
  options: { autoCreateTopics?: boolean } = {},
): Promise<Harness> {
  const broker = await FakeBroker.start();
  const queue = new DriftmqQueue({
    host: "127.0.0.1",
    port: broker.port,
    pollIntervalMs: 20,
    connectTimeoutMs: 1_000,
    requestTimeoutMs: 2_000,
    ...options,
  });

  const controllers: AbortController[] = [];
  t.after(async () => {
    for (const controller of controllers) controller.abort();
    await queue.close();
    await broker.close();
  });

  return {
    broker,
    queue,
    async consume(handler, prefetch = 1) {
      const controller = new AbortController();
      controllers.push(controller);
      await queue.consume(handler, { queue: QUEUE, prefetch, signal: controller.signal });
    },
  };
}

test("capabilities는 driftmq가 실제로 제공하는 것만 선언한다", async (t) => {
  const { queue } = await harness(t);

  assert.equal(queue.name, "driftmq");
  assert.deepEqual(queue.capabilities, {
    nativeDelay: false,
    redelivery: true,
    visibilityTimeoutMs: null,
    nativeDeadLetter: true,
  });
});

test("enqueue는 토픽을 만들고 envelope을 JSON body + 헤더로 publish한다", async (t) => {
  const { broker, queue } = await harness(t);
  const env = envelope();

  await queue.enqueue(env);

  assert.deepEqual(broker.topics(), [TOPIC]);
  const record = broker.records(TOPIC)[0];
  assert.ok(record !== undefined);
  assert.deepEqual(JSON.parse(record.payload.toString("utf8")), env);
  assert.equal(
    record.headers.find((header) => header.key === "job-id")?.value.toString("utf8"),
    env.jobId,
  );
  assert.equal(broker.requests.filter((request) => request.kind === "topic-create").length, 1);

  // 같은 큐로 다시 넣어도 토픽 생성을 반복하지 않는다.
  await queue.enqueue(envelope());
  assert.equal(broker.requests.filter((request) => request.kind === "topic-create").length, 1);
});

test("enqueue → consume → ack: 한 번 전달되고 ack 뒤에는 재전달되지 않는다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];

  await h.consume(async (delivery) => {
    seen.push(delivery);
    await h.queue.ack(delivery.receipt);
  });

  await h.queue.enqueue(envelope({ jobId: "J1" }));
  await waitFor(() => seen.length === 1, "첫 전달");

  assert.equal(seen[0]?.envelope.jobId, "J1");
  assert.equal(seen[0]?.deliveryCount, 1);
  assert.equal(seen[0]?.receipt.adapter, "driftmq");

  await waitFor(() => h.broker.committed(TOPIC, CONSUMER) === 1n, "커밋 전진");
  await sleep(150);
  assert.equal(seen.length, 1, "ack된 메시지는 다시 오지 않는다");
});

test("ack은 offset 단위다 — 확정한 offset만 재전달에서 빠진다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];

  await h.consume(async (delivery) => {
    seen.push(delivery);
  }, 2);

  await h.queue.enqueue(envelope({ jobId: "J0" }));
  await h.queue.enqueue(envelope({ jobId: "J1" }));
  await waitFor(() => seen.length === 2, "두 건 전달");

  const [first, second] = seen;
  assert.ok(first !== undefined && second !== undefined);

  await h.queue.ack(second.receipt);
  assert.equal(h.broker.committed(TOPIC, CONSUMER), 0n, "앞 offset이 비어 있으면 커밋은 멈춘다");
  await h.queue.nack(first.receipt);

  // 연결이 끊기면 미ACK 메시지가 ack timeout을 기다리지 않고 즉시 재전달 대상이 된다.
  h.broker.dropConnections();
  await waitFor(() => seen.length === 3, "미ACK 메시지 재전달");
  await sleep(150);

  assert.deepEqual(
    seen.map((delivery) => delivery.envelope.jobId),
    ["J0", "J1", "J0"],
    "ack된 offset은 다시 오지 않는다",
  );
  assert.equal(seen[2]?.deliveryCount, 2, "재전달 회차는 예약 헤더에서 온다");
});

test("아직 들고 있는 전달은 재연결해도 두 번 실행하지 않는다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];

  await h.consume(async (delivery) => {
    seen.push(delivery);
  });

  await h.queue.enqueue(envelope({ jobId: "J0" }));
  await waitFor(() => seen.length === 1, "첫 전달");

  h.broker.dropConnections();
  await sleep(300);
  assert.equal(seen.length, 1, "같은 offset의 중복 전달은 걸러진다");

  const first = seen[0];
  assert.ok(first !== undefined);
  await h.queue.ack(first.receipt);
  await waitFor(() => h.broker.committed(TOPIC, CONSUMER) === 1n, "재연결 뒤에도 ack이 먹힌다");
});

test("prefetch를 넘겨 전달하지 않는다", async (t) => {
  const h = await harness(t);
  const held: Delivery[] = [];

  await h.consume(async (delivery) => {
    held.push(delivery);
  }, 1);

  await h.queue.enqueue(envelope());
  await h.queue.enqueue(envelope());
  await h.queue.enqueue(envelope());
  await waitFor(() => held.length === 1, "첫 전달");

  await sleep(150);
  assert.equal(held.length, 1, "미ACK 전달이 prefetch만큼 차면 더 가져오지 않는다");

  const first = held[0];
  assert.ok(first !== undefined);
  await h.queue.ack(first.receipt);
  await waitFor(() => held.length === 2, "슬롯이 비면 다음 전달");
});

test("nack(requeue)은 ack하지 않고 슬롯만 반납한다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];

  await h.consume(async (delivery) => {
    seen.push(delivery);
    if (seen.length === 1) await h.queue.nack(delivery.receipt);
  });

  await h.queue.enqueue(envelope({ jobId: "J0" }));
  await waitFor(() => seen.length === 1, "첫 전달");

  assert.equal(h.broker.committed(TOPIC, CONSUMER), 0n);
  assert.equal(
    h.broker.requests.some((request) => request.kind === "ack"),
    false,
    "nack은 ACK 프레임을 보내지 않는다",
  );

  h.broker.dropConnections();
  await waitFor(() => seen.length === 2, "재전달");
  assert.equal(seen[1]?.envelope.jobId, "J0");
});

test("nack(requeue: false)는 메시지를 버리지 않고 dead 토픽으로 보낸다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];

  await h.consume(async (delivery) => {
    seen.push(delivery);
    await h.queue.nack(delivery.receipt, { requeue: false });
  });

  await h.queue.enqueue(envelope({ jobId: "J0" }));
  await waitFor(() => h.broker.records(DEAD_TOPIC).length === 1, "dead 토픽 기록");
  await waitFor(() => h.broker.committed(TOPIC, CONSUMER) === 1n, "원본 ack");

  const dead = h.broker.records(DEAD_TOPIC)[0];
  assert.equal(
    dead?.headers.find((header) => header.key === "dead-reason")?.value.toString("utf8"),
    "fatal",
  );
  assert.equal(JSON.parse(String(dead?.payload)).jobId, "J0");
});

test("deadLetter는 사유 헤더와 함께 dead 토픽에 남긴다", async (t) => {
  const h = await harness(t);
  const env = envelope({ jobId: "J9" });

  await h.queue.deadLetter(env, {
    kind: "attempts-exhausted",
    attempt: 3,
    error: "upstream 503",
  });

  const record = h.broker.records(DEAD_TOPIC)[0];
  assert.ok(record !== undefined);
  assert.deepEqual(JSON.parse(record.payload.toString("utf8")), env);

  const headers = Object.fromEntries(
    record.headers.map((header) => [header.key, header.value.toString("utf8")]),
  );
  assert.equal(headers["dead-reason"], "attempts-exhausted");
  assert.equal(headers["dead-error"], "attempt 3: upstream 503");
  assert.equal(headers["job-id"], "J9");
  assert.ok(Number(headers["dead-at"]) > 0);
});

test("파싱할 수 없는 메시지는 dead로 보내고 ack한다 — poison message를 끊는다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];

  h.broker.createTopic(TOPIC);
  h.broker.append(
    TOPIC,
    [{ key: "job-id", value: Buffer.from("J-bad", "utf8") }],
    Buffer.from("{oops", "utf8"),
  );

  await h.consume(async (delivery) => {
    seen.push(delivery);
  });

  await waitFor(() => h.broker.records(DEAD_TOPIC).length === 1, "dead 토픽 이동");
  await waitFor(() => h.broker.committed(TOPIC, CONSUMER) === 1n, "원본 ack");

  const dead = h.broker.records(DEAD_TOPIC)[0];
  const headers = Object.fromEntries(
    (dead?.headers ?? []).map((header) => [header.key, header.value.toString("utf8")]),
  );
  assert.equal(headers["dead-reason"], "undecodable");
  assert.equal(headers["job-id"], "J-bad", "원본 사용자 헤더는 보존한다");
  assert.equal(seen.length, 0, "handler는 실행되지 않는다");
});

test("MALFORMED_FRAME은 연결을 끊지만 다음 요청이 재연결한다", async (t) => {
  const h = await harness(t);

  await h.queue.enqueue(envelope({ jobId: "J0" }));
  h.broker.failNextWith = ErrorCode.MALFORMED_FRAME;

  await assert.rejects(() => h.queue.enqueue(envelope({ jobId: "J1" })), /MALFORMED_FRAME/);

  await h.queue.enqueue(envelope({ jobId: "J2" }));
  assert.deepEqual(
    h.broker.records(TOPIC).map((record) => JSON.parse(record.payload.toString("utf8")).jobId),
    ["J0", "J2"],
  );
});

test("UNKNOWN_TOPIC이어도 폴링 루프는 죽지 않는다", async (t) => {
  const h = await harness(t, { autoCreateTopics: false });
  const seen: Delivery[] = [];

  await h.consume(async (delivery) => {
    seen.push(delivery);
    await h.queue.ack(delivery.receipt);
  });

  await sleep(120);
  assert.equal(seen.length, 0);

  // 토픽이 나중에 생겨도 루프가 이어받는다.
  h.broker.createTopic(TOPIC);
  h.broker.append(TOPIC, [], Buffer.from(JSON.stringify(envelope({ jobId: "late" })), "utf8"));

  await waitFor(() => seen.length === 1, "토픽 생성 후 전달");
  assert.equal(seen[0]?.envelope.jobId, "late");
});

test("close 후에는 enqueue와 consume이 거부된다", async (t) => {
  const h = await harness(t);
  await h.queue.close();

  await assert.rejects(() => h.queue.enqueue(envelope()), /close/);
  await assert.rejects(
    () =>
      h.queue.consume(async () => {}, {
        queue: QUEUE,
        prefetch: 1,
        signal: new AbortController().signal,
      }),
    /close/,
  );
});

test("다른 어댑터의 receipt는 거부한다", async (t) => {
  const h = await harness(t);
  await assert.rejects(() => h.queue.ack({ adapter: "memory", token: 1 }), /memory/);
});

test("signal을 abort하면 더 이상 전달받지 않는다", async (t) => {
  const h = await harness(t);
  const seen: Delivery[] = [];
  const controller = new AbortController();

  await h.queue.consume(
    async (delivery) => {
      seen.push(delivery);
      await h.queue.ack(delivery.receipt);
    },
    { queue: QUEUE, prefetch: 1, signal: controller.signal },
  );

  controller.abort();
  await sleep(50);
  await h.queue.enqueue(envelope());
  await sleep(150);

  assert.equal(seen.length, 0);
});
