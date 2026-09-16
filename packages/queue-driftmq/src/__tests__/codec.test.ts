import assert from "node:assert/strict";
import test from "node:test";

import { decodeBody, encodeFrame } from "../wire/codec.js";
import { ByteReader, ByteWriter } from "../wire/cursor.js";
import { MessageType, PROTOCOL_VERSION } from "../wire/protocol.js";
import type { WireHeader } from "../wire/types.js";

function bodyOf(frame: Buffer): Buffer {
  assert.equal(frame.readUInt32BE(0), frame.length - 4, "길이 prefix는 body 바이트 수다");
  return frame.subarray(4);
}

function response(type: number, correlationId: number, write: (out: ByteWriter) => void): Buffer {
  const out = new ByteWriter();
  out.u8(PROTOCOL_VERSION).u8(type).i32(correlationId);
  write(out);
  return out.toBuffer();
}

test("ACK 프레임은 바이트 단위로 고정된 레이아웃을 갖는다", () => {
  const frame = encodeFrame(
    { kind: "ack", topic: "t", consumerId: "c", offset: 1n },
    7,
  );

  assert.equal(
    frame.toString("hex"),
    "00000014" + // bodyLen = 20
      "01" + // version
      "03" + // type = ACK
      "00000007" + // correlationId
      "0001" +
      "74" + // topic "t"
      "0001" +
      "63" + // consumerId "c"
      "0000000000000001", // offset i64
  );
});

test("PUBLISH는 topic → headers → payload 순서로 나간다", () => {
  const headers: WireHeader[] = [{ key: "job-id", value: Buffer.from("J1", "utf8") }];
  const payload = Buffer.from('{"v":1}', "utf8");

  const reader = new ByteReader(
    bodyOf(encodeFrame({ kind: "publish", topic: "taskrail.default", headers, payload }, 3)),
  );

  assert.equal(reader.u8(), PROTOCOL_VERSION);
  assert.equal(reader.u8(), MessageType.PUBLISH);
  assert.equal(reader.i32(), 3);
  assert.equal(reader.string(), "taskrail.default");

  const decodedHeaders = reader.headers();
  assert.equal(decodedHeaders.length, 1);
  assert.equal(decodedHeaders[0]?.key, "job-id");
  assert.equal(decodedHeaders[0]?.value.toString("utf8"), "J1");

  assert.deepEqual(reader.bytes(), payload);
  assert.equal(reader.remaining, 0);
});

test("FETCH는 maxMessages를 i32로 싣는다", () => {
  const reader = new ByteReader(
    bodyOf(
      encodeFrame(
        { kind: "fetch", topic: "taskrail.default", consumerId: "taskrail-default", maxMessages: 10 },
        1,
      ),
    ),
  );

  reader.u8();
  assert.equal(reader.u8(), MessageType.FETCH);
  assert.equal(reader.i32(), 1);
  assert.equal(reader.string(), "taskrail.default");
  assert.equal(reader.string(), "taskrail-default");
  assert.equal(reader.i32(), 10);
});

test("PUBLISH_OK은 i64 offset 하나만 갖는다", () => {
  const decoded = decodeBody(
    response(MessageType.PUBLISH_OK, 42, (out) => out.i64(9_007_199_254_740_993n)),
  );

  assert.equal(decoded.kind, "publish-ok");
  assert.equal(decoded.correlationId, 42);
  assert.equal(decoded.kind === "publish-ok" ? decoded.offset : 0n, 9_007_199_254_740_993n);
});

test("ACK_OK의 body는 공통 머리 6바이트가 전부다", () => {
  const body = response(MessageType.ACK_OK, 5, () => {});

  assert.equal(body.length, 6);
  assert.deepEqual(decodeBody(body), { kind: "ack-ok", correlationId: 5 });
});

test("FETCH_OK 레코드를 offset/timestamp/headers/payload 순으로 읽는다", () => {
  const payload = Buffer.from("hello", "utf8");
  const decoded = decodeBody(
    response(MessageType.FETCH_OK, 9, (out) => {
      out.i32(2);
      out
        .i64(0n)
        .i64(1_700_000_000_000n)
        .headers([{ key: "job-id", value: Buffer.from("J1", "utf8") }])
        .bytes(payload);
      out.i64(1n).i64(1_700_000_000_001n).headers([]).bytes(Buffer.alloc(0));
    }),
  );

  assert.equal(decoded.kind, "fetch-ok");
  if (decoded.kind !== "fetch-ok") return;

  assert.equal(decoded.records.length, 2);
  assert.equal(decoded.records[0]?.offset, 0n);
  assert.equal(decoded.records[0]?.timestamp, 1_700_000_000_000n);
  assert.equal(decoded.records[0]?.headers[0]?.key, "job-id");
  assert.deepEqual(decoded.records[0]?.payload, payload);
  assert.equal(decoded.records[1]?.offset, 1n);
  assert.equal(decoded.records[1]?.payload.length, 0);
});

test("메시지가 없으면 count 0인 정상 FETCH_OK다", () => {
  const decoded = decodeBody(response(MessageType.FETCH_OK, 1, (out) => out.i32(0)));
  assert.equal(decoded.kind === "fetch-ok" ? decoded.records.length : -1, 0);
});

test("ERROR는 errorCode u16과 메시지를 갖는다", () => {
  const decoded = decodeBody(
    response(MessageType.ERROR, 11, (out) => {
      out.u16(1).string("unknown topic: nope");
    }),
  );

  assert.deepEqual(decoded, {
    kind: "error",
    correlationId: 11,
    code: 1,
    message: "unknown topic: nope",
  });
});

test("version 바이트가 다르면 거부한다", () => {
  const body = response(MessageType.ACK_OK, 1, () => {});
  body.writeUInt8(2, 0);

  assert.throws(() => decodeBody(body), /프로토콜 버전/);
});

test("알 수 없는 응답 type은 거부한다", () => {
  assert.throws(() => decodeBody(response(200, 1, () => {})), /알 수 없는 응답 type/);
});

test("공통 머리보다 짧은 body는 거부한다", () => {
  assert.throws(() => decodeBody(Buffer.alloc(5)), /공통 머리/);
});

test("잘린 body는 거부한다", () => {
  const body = response(MessageType.PUBLISH_OK, 1, (out) => out.i64(1n));
  assert.throws(() => decodeBody(body.subarray(0, 10)), /잘렸다/);
});

test("터무니없는 count는 손상으로 본다", () => {
  assert.throws(
    () => decodeBody(response(MessageType.FETCH_OK, 1, (out) => out.i32(1_000_000))),
    /count/,
  );
});

test("65535바이트를 넘는 string은 인코딩하지 않는다", () => {
  assert.throws(
    () => encodeFrame({ kind: "topic-create", topic: "x".repeat(70_000) }, 1),
    /string/,
  );
});

test("bytes 길이가 음수면 거부한다", () => {
  const out = new ByteWriter();
  out.u8(PROTOCOL_VERSION).u8(MessageType.FETCH_OK).i32(1);
  out.i32(1).i64(0n).i64(0n).headers([]).i32(-1);

  assert.throws(() => decodeBody(out.toBuffer()), /음수/);
});
