import assert from "node:assert/strict";
import test from "node:test";

import { WIRE_HEADERS } from "@taskrail/core";
import type { JobEnvelope } from "@taskrail/core";

import { withoutReservedHeaders } from "../dead/dead-letter.js";
import {
  assertNoReservedHeaders,
  decodeEnvelope,
  encodeEnvelope,
  toHeaders,
} from "../message/envelope-codec.js";
import { readAttemptCount, readBrokerDeadLetterInfo } from "../message/reserved.js";
import { deadTopic, jobTopic } from "../queue/topics.js";
import { isValidTopic } from "../wire/topic.js";

const ENVELOPE: JobEnvelope = {
  v: 1,
  jobId: "J1",
  name: "charge",
  queue: "default",
  payload: { amount: 10 },
  attempt: 2,
  maxAttempts: 3,
  enqueuedAt: 1_700_000_000_000,
  notBefore: 1_700_000_000_500,
  timeoutMs: 30_000,
  backoff: { type: "fixed", delayMs: 10, jitter: "none" },
};

function text(headers: readonly { key: string; value: Buffer }[], key: string): string | undefined {
  return headers.find((header) => header.key === key)?.value.toString("utf8");
}

test("envelope은 JSON body로, 헤더는 그 사본으로 나간다", () => {
  const { headers, payload } = encodeEnvelope(ENVELOPE);

  assert.equal(text(headers, WIRE_HEADERS.version), "1");
  assert.equal(text(headers, WIRE_HEADERS.jobId), "J1");
  assert.equal(text(headers, WIRE_HEADERS.jobName), "charge");
  assert.equal(text(headers, WIRE_HEADERS.attempt), "2");
  assert.equal(text(headers, WIRE_HEADERS.notBefore), "1700000000500");
  assert.deepEqual(decodeEnvelope(payload), ENVELOPE);
});

test("taskrail 헤더 키는 브로커 예약 접두사와 겹치지 않는다", () => {
  const { headers } = encodeEnvelope(ENVELOPE);
  for (const header of headers) {
    assert.ok(
      !header.key.toLowerCase().startsWith("x-driftmq-"),
      `${header.key}는 예약 접두사를 쓰면 안 된다`,
    );
  }
});

test("예약 접두사 헤더는 대소문자를 무시하고 거부한다", () => {
  assert.throws(
    () => assertNoReservedHeaders(toHeaders({ "X-DriftMQ-Attempt": "3" })),
    /예약 네임스페이스/,
  );
  assert.doesNotThrow(() => assertNoReservedHeaders(toHeaders({ "job-id": "J1" })));
});

test("JobEnvelope이 아닌 body는 undefined로 돌려준다", () => {
  assert.equal(decodeEnvelope(Buffer.from("not json", "utf8")), undefined);
  assert.equal(decodeEnvelope(Buffer.from('{"hello":"world"}', "utf8")), undefined);
  assert.equal(decodeEnvelope(Buffer.from("null", "utf8")), undefined);
});

test("attemptCount는 예약 헤더에서 파생되고 없으면 1이다", () => {
  assert.equal(readAttemptCount([]), 1);
  assert.equal(readAttemptCount(toHeaders({ "x-driftmq-attempt": "3" })), 3);
  assert.equal(readAttemptCount(toHeaders({ "x-driftmq-attempt": "?" })), 1);
});

test("브로커 DLQ 메타데이터는 헤더에서 읽는다", () => {
  assert.equal(readBrokerDeadLetterInfo(toHeaders({ "job-id": "J1" })), undefined);

  const info = readBrokerDeadLetterInfo(
    toHeaders({
      "x-driftmq-dlq-source-topic": "taskrail.default",
      "x-driftmq-dlq-source-offset": "12",
      "x-driftmq-dlq-consumer-id": "taskrail-default",
      "x-driftmq-dlq-attempts": "5",
      "x-driftmq-dlq-failed-at": "1700000000000",
    }),
  );

  assert.deepEqual(info, {
    sourceTopic: "taskrail.default",
    sourceOffset: 12,
    consumerId: "taskrail-default",
    attempts: 5,
    failedAt: 1_700_000_000_000,
  });
});

test("dead로 재publish할 때 예약 헤더를 떼어낸다", () => {
  const kept = withoutReservedHeaders(
    toHeaders({ "job-id": "J1", "x-driftmq-attempt": "2" }),
  );

  assert.deepEqual(
    kept.map((header) => header.key),
    ["job-id"],
  );
});

test("토픽 이름 규칙을 클라이언트에서 먼저 검증한다", () => {
  assert.equal(jobTopic("taskrail", "default"), "taskrail.default");
  assert.equal(deadTopic("taskrail", "default"), "taskrail.default.dead");

  assert.ok(isValidTopic("taskrail.default.dlq"));
  assert.ok(!isValidTopic("."));
  assert.ok(!isValidTopic(".."));
  assert.ok(!isValidTopic("bad topic"));
  assert.ok(!isValidTopic("bad/topic"));
  assert.ok(!isValidTopic(""));
  assert.ok(!isValidTopic("x".repeat(256)));

  assert.throws(() => jobTopic("taskrail", "with space"), /driftmq 규칙/);
});
