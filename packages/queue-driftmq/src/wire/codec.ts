import { ProtocolError } from "../support/errors.js";
import { ByteReader, ByteWriter } from "./cursor.js";
import {
  COMMON_HEADER_BYTES,
  MAX_FRAME_BYTES,
  MessageType,
  PROTOCOL_VERSION,
} from "./protocol.js";
import type { FetchedRecord, WireRequest, WireResponse } from "./types.js";

/** 요청 하나를 길이 prefix까지 붙인 완성된 프레임으로 만든다. */
export function encodeFrame(request: WireRequest, correlationId: number): Buffer {
  const body = encodeBody(request, correlationId);
  if (body.length > MAX_FRAME_BYTES) {
    throw new ProtocolError(
      `프레임 body가 최대 크기를 넘었다: ${body.length} > ${MAX_FRAME_BYTES}`,
    );
  }

  const frame = Buffer.allocUnsafe(4 + body.length);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, 4);
  return frame;
}

export function encodeBody(request: WireRequest, correlationId: number): Buffer {
  const out = new ByteWriter();
  out.u8(PROTOCOL_VERSION).u8(requestType(request)).i32(correlationId);

  switch (request.kind) {
    case "publish":
      out.string(request.topic).headers(request.headers).bytes(request.payload);
      break;
    case "fetch":
      out.string(request.topic).string(request.consumerId).i32(request.maxMessages);
      break;
    case "ack":
      out.string(request.topic).string(request.consumerId).i64(request.offset);
      break;
    case "topic-create":
      out.string(request.topic);
      break;
  }

  return out.toBuffer();
}

function requestType(request: WireRequest): number {
  switch (request.kind) {
    case "publish":
      return MessageType.PUBLISH;
    case "fetch":
      return MessageType.FETCH;
    case "ack":
      return MessageType.ACK;
    case "topic-create":
      return MessageType.TOPIC_CREATE;
  }
}

/** 길이 prefix를 뗀 body만 받는다. 버전 불일치는 조용히 넘어가지 않고 즉시 실패시킨다. */
export function decodeBody(body: Buffer): WireResponse {
  if (body.length < COMMON_HEADER_BYTES) {
    throw new ProtocolError(`body가 공통 머리(${COMMON_HEADER_BYTES}B)보다 짧다`);
  }

  const reader = new ByteReader(body);
  const version = reader.u8();
  const type = reader.u8();
  const correlationId = reader.i32();

  if (version !== PROTOCOL_VERSION) {
    throw new ProtocolError(`지원하지 않는 프로토콜 버전: ${version}`);
  }

  switch (type) {
    case MessageType.PUBLISH_OK:
      return { kind: "publish-ok", correlationId, offset: reader.i64() };
    case MessageType.FETCH_OK:
      return { kind: "fetch-ok", correlationId, records: decodeRecords(reader) };
    case MessageType.ACK_OK:
      return { kind: "ack-ok", correlationId };
    case MessageType.TOPIC_CREATE_OK:
      return { kind: "topic-create-ok", correlationId };
    case MessageType.ERROR:
      return {
        kind: "error",
        correlationId,
        code: reader.u16(),
        message: reader.string(),
      };
    default:
      throw new ProtocolError(`알 수 없는 응답 type: ${type}`);
  }
}

function decodeRecords(reader: ByteReader): FetchedRecord[] {
  const count = reader.i32();
  if (count < 0 || count > reader.remaining + 1) {
    throw new ProtocolError(`FETCH_OK count ${count}가 남은 ${reader.remaining}바이트를 넘는다`);
  }

  const records: FetchedRecord[] = [];
  for (let i = 0; i < count; i += 1) {
    records.push({
      offset: reader.i64(),
      timestamp: reader.i64(),
      headers: reader.headers(),
      payload: reader.bytes(),
    });
  }
  return records;
}
