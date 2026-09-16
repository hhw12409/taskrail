import { toWireHeaders } from "@taskrail/core";
import type { JobEnvelope } from "@taskrail/core";
import { isJobEnvelope } from "@taskrail/core/internal";

import { DriftmqError } from "../support/errors.js";
import type { WireHeader } from "../wire/types.js";
import { isReservedHeaderKey } from "./reserved.js";

export interface EncodedMessage {
  readonly headers: readonly WireHeader[];
  readonly payload: Buffer;
}

/** body는 envelope JSON 전체, 헤더는 그 사본이다. 불일치하면 body가 진실이다. */
export function encodeEnvelope(
  envelope: JobEnvelope,
  extraHeaders: Readonly<Record<string, string>> = {},
): EncodedMessage {
  const headers = toHeaders({ ...toWireHeaders(envelope), ...extraHeaders });
  assertNoReservedHeaders(headers);
  return { headers, payload: Buffer.from(JSON.stringify(envelope), "utf8") };
}

/** 파싱할 수 없는 메시지는 실행 대상이 아니다 — 호출자가 dead로 보내고 ack해야 한다. */
export function decodeEnvelope(payload: Buffer): JobEnvelope | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload.toString("utf8"));
  } catch {
    return undefined;
  }
  return isJobEnvelope(parsed) ? parsed : undefined;
}

export function toHeaders(values: Readonly<Record<string, string>>): WireHeader[] {
  return Object.entries(values).map(([key, value]) => ({
    key,
    value: Buffer.from(value, "utf8"),
  }));
}

export function assertNoReservedHeaders(headers: readonly WireHeader[]): void {
  for (const header of headers) {
    if (isReservedHeaderKey(header.key)) {
      throw new DriftmqError(
        `헤더 키 "${header.key}"는 브로커 예약 네임스페이스다 — publish가 거부되고 연결이 끊긴다`,
      );
    }
  }
}
