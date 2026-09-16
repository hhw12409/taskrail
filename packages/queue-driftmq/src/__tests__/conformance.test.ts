import assert from "node:assert/strict";
import test from "node:test";

import type { Delivery, JobEnvelope } from "@taskrail/core";
import { noopLogger } from "@taskrail/core/internal";

import { DriftConnection } from "../connection/connection.js";
import { DriftmqQueue } from "../index.js";
import { BrokerError } from "../support/errors.js";
import type { WireResponse } from "../wire/types.js";

/**
 * 실제 브로커를 상대로 도는 유일한 테스트다. 기본 실행에서는 건너뛴다:
 *   TASKRAIL_DRIFTMQ_HOST=127.0.0.1 TASKRAIL_DRIFTMQ_PORT=9092 npm test
 */
const HOST = process.env["TASKRAIL_DRIFTMQ_HOST"];
const PORT = Number(process.env["TASKRAIL_DRIFTMQ_PORT"] ?? 9092);
const RUN_ID = `c${Date.now().toString(36)}`;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function envelope(queue: string, jobId: string): JobEnvelope {
  const now = Date.now();
  return {
    v: 1,
    jobId,
    name: "conformance",
    queue,
    payload: { hello: "driftmq" },
    attempt: 1,
    maxAttempts: 1,
    enqueuedAt: now,
    notBefore: now,
    timeoutMs: 30_000,
    backoff: { type: "fixed", delayMs: 10, jitter: "none" },
  };
}

function connection(label: string): DriftConnection {
  return new DriftConnection({
    host: HOST ?? "127.0.0.1",
    port: PORT,
    connectTimeoutMs: 5_000,
    requestTimeoutMs: 10_000,
    logger: noopLogger,
    label,
  });
}

async function createTopic(link: DriftConnection, topic: string): Promise<void> {
  const response = await link.send({ kind: "topic-create", topic });
  assert.equal(response.kind, "topic-create-ok");
}

function records(response: WireResponse): readonly { offset: bigint; payload: Buffer }[] {
  assert.equal(response.kind, "fetch-ok");
  return response.kind === "fetch-ok" ? response.records : [];
}

test(
  "conformance: 실제 driftmq 브로커와의 왕복",
  {
    skip:
      HOST === undefined
        ? "TASKRAIL_DRIFTMQ_HOST가 없다 — 실제 브로커 대상 스모크 테스트를 건너뛴다"
        : false,
  },
  async (t) => {
    await t.test("TOPIC_CREATE → PUBLISH → FETCH → ACK 왕복", async () => {
      const topic = `taskrail.${RUN_ID}-roundtrip`;
      const consumerId = "taskrail-conformance";
      const link = connection("conformance");

      try {
        // version 바이트가 브로커와 다르면 여기서 MALFORMED_FRAME이 난다.
        await createTopic(link, topic);

        const payload = Buffer.from("hello", "utf8");
        const published = await link.send({
          kind: "publish",
          topic,
          headers: [{ key: "job-id", value: Buffer.from("J1", "utf8") }],
          payload,
        });
        assert.equal(published.kind, "publish-ok");

        const fetched = records(
          await link.send({ kind: "fetch", topic, consumerId, maxMessages: 10 }),
        );
        assert.equal(fetched.length, 1);
        assert.deepEqual(fetched[0]?.payload, payload);

        const acked = await link.send({
          kind: "ack",
          topic,
          consumerId,
          offset: fetched[0]?.offset ?? 0n,
        });
        assert.equal(acked.kind, "ack-ok");

        const after = records(
          await link.send({ kind: "fetch", topic, consumerId, maxMessages: 10 }),
        );
        assert.equal(after.length, 0, "ack한 메시지는 다시 오지 않는다");
      } finally {
        await link.close();
      }
    });

    await t.test("ACK는 offset 하나만 확정한다", async () => {
      const topic = `taskrail.${RUN_ID}-sparse`;
      const consumerId = "taskrail-conformance";
      const first = connection("sparse-1");

      try {
        await createTopic(first, topic);
        for (const body of ["a", "b"]) {
          await first.send({
            kind: "publish",
            topic,
            headers: [],
            payload: Buffer.from(body, "utf8"),
          });
        }

        const fetched = records(
          await first.send({ kind: "fetch", topic, consumerId, maxMessages: 10 }),
        );
        assert.equal(fetched.length, 2);

        // 뒤쪽 offset만 확정한다.
        await first.send({ kind: "ack", topic, consumerId, offset: fetched[1]?.offset ?? 1n });
      } finally {
        await first.close();
      }

      // 연결을 끊으면 미ACK 메시지가 즉시 재전달 대상이 된다.
      const second = connection("sparse-2");
      try {
        await sleep(100);
        const again = records(
          await second.send({ kind: "fetch", topic, consumerId, maxMessages: 10 }),
        );
        assert.deepEqual(
          again.map((record) => record.payload.toString("utf8")),
          ["a"],
          "ACK가 누적(offset까지 전부)이었다면 여기서 아무것도 오지 않는다",
        );
      } finally {
        await second.close();
      }
    });

    await t.test("x-driftmq- 접두사 헤더에 대한 브로커 반응", async () => {
      const topic = `taskrail.${RUN_ID}-reserved`;
      const link = connection("reserved");

      try {
        await createTopic(link, topic);
        await link.send({
          kind: "publish",
          topic,
          headers: [{ key: "x-driftmq-attempt", value: Buffer.from("2", "utf8") }],
          payload: Buffer.from("x", "utf8"),
        });
        t.diagnostic("이 브로커는 예약 접두사 헤더를 거부하지 않는다 (v0.1 계열)");
      } catch (error) {
        assert.ok(error instanceof BrokerError, `예상 밖의 실패: ${String(error)}`);
        t.diagnostic(`예약 접두사 헤더를 ${error.codeName}로 거부한다`);
      } finally {
        await link.close();
      }
    });

    await t.test("DriftmqQueue: enqueue → consume → ack", async () => {
      const queue = `${RUN_ID}-queue`;
      const driftmq = new DriftmqQueue({
        host: HOST ?? "127.0.0.1",
        port: PORT,
        pollIntervalMs: 50,
      });
      const controller = new AbortController();
      const seen: Delivery[] = [];

      try {
        await driftmq.consume(
          async (delivery) => {
            seen.push(delivery);
            await driftmq.ack(delivery.receipt);
          },
          { queue, prefetch: 4, signal: controller.signal },
        );

        const env = envelope(queue, "J-integration");
        await driftmq.enqueue(env);

        const deadline = Date.now() + 10_000;
        while (seen.length === 0 && Date.now() < deadline) await sleep(50);

        assert.equal(seen.length, 1, "10초 안에 전달되지 않았다");
        assert.deepEqual(seen[0]?.envelope, env);
        assert.equal(seen[0]?.deliveryCount, 1);

        await sleep(500);
        assert.equal(seen.length, 1, "ack 뒤에 재전달이 없어야 한다");
      } finally {
        controller.abort();
        await driftmq.close();
      }
    });
  },
);
