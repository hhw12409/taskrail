import type { JobEnvelope } from "./envelope.js";

/**
 * 경계를 넘는 헤더 키는 여기 한 곳에서만 정의하고 어댑터가 이 상수를 참조한다.
 * `x-driftmq-` 접두사는 브로커 예약 네임스페이스라 publish가 거부된다 — 절대 쓰지 않는다.
 */
export const WIRE_HEADERS = {
  version: "taskrail-v",
  jobId: "job-id",
  jobName: "job-name",
  attempt: "job-attempt",
  notBefore: "job-not-before",
} as const;

export type WireHeaderKey = (typeof WIRE_HEADERS)[keyof typeof WIRE_HEADERS];

/** 헤더는 body(envelope JSON)의 사본이다. 불일치 시 body가 진실의 원천이다. */
export function toWireHeaders(envelope: JobEnvelope): Record<WireHeaderKey, string> {
  return {
    [WIRE_HEADERS.version]: String(envelope.v),
    [WIRE_HEADERS.jobId]: envelope.jobId,
    [WIRE_HEADERS.jobName]: envelope.name,
    [WIRE_HEADERS.attempt]: String(envelope.attempt),
    [WIRE_HEADERS.notBefore]: String(envelope.notBefore),
  } as Record<WireHeaderKey, string>;
}
