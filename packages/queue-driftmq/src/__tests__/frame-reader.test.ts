import assert from "node:assert/strict";
import test from "node:test";

import { encodeFrame } from "../wire/codec.js";
import { FrameReader } from "../wire/frame-reader.js";
import { MAX_FRAME_BYTES } from "../wire/protocol.js";

function ackFrame(correlationId: number): Buffer {
  return encodeFrame({ kind: "ack", topic: "t", consumerId: "c", offset: 1n }, correlationId);
}

test("완전한 프레임이 없으면 아무것도 소비하지 않는다", () => {
  const reader = new FrameReader();
  const frame = ackFrame(1);

  reader.push(frame.subarray(0, 3));
  assert.equal(reader.next(), undefined, "길이 prefix도 다 오지 않았다");

  reader.push(frame.subarray(3, frame.length - 1));
  assert.equal(reader.next(), undefined, "body가 1바이트 모자란다");

  reader.push(frame.subarray(frame.length - 1));
  assert.deepEqual(reader.next(), frame.subarray(4));
  assert.equal(reader.next(), undefined);
});

test("1바이트씩 흘려 넣어도 프레임을 재조립한다", () => {
  const reader = new FrameReader();
  const frame = ackFrame(7);

  for (let i = 0; i < frame.length - 1; i += 1) {
    reader.push(frame.subarray(i, i + 1));
    assert.equal(reader.next(), undefined);
  }

  reader.push(frame.subarray(frame.length - 1));
  assert.deepEqual(reader.next(), frame.subarray(4));
});

test("한 청크에 여러 프레임이 붙어 와도 순서대로 꺼낸다", () => {
  const reader = new FrameReader();
  reader.push(Buffer.concat([ackFrame(1), ackFrame(2), ackFrame(3).subarray(0, 5)]));

  const first = reader.next();
  const second = reader.next();
  assert.ok(first !== undefined && second !== undefined);
  assert.equal(first.readInt32BE(2), 1);
  assert.equal(second.readInt32BE(2), 2);
  assert.equal(reader.next(), undefined, "세 번째는 아직 미완성이다");

  reader.push(ackFrame(3).subarray(5));
  assert.equal(reader.next()?.readInt32BE(2), 3);
});

test("최대 크기를 넘는 길이 prefix는 거부한다", () => {
  const reader = new FrameReader();
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(MAX_FRAME_BYTES + 1, 0);

  reader.push(prefix);
  assert.throws(() => reader.next(), /프레임이 너무 크다/);
});

test("reset은 남은 바이트를 버린다", () => {
  const reader = new FrameReader();
  reader.push(ackFrame(1).subarray(0, 6));
  reader.reset();

  assert.equal(reader.buffered, 0);
  assert.equal(reader.next(), undefined);
});
