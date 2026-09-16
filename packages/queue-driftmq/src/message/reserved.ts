import type { WireHeader } from "../wire/types.js";

/** 브로커 예약 네임스페이스. 여기 속한 키를 publish하면 MALFORMED_FRAME으로 거부된다. */
export const RESERVED_HEADER_PREFIX = "x-driftmq-";

export const DRIFTMQ_HEADERS = {
  attempt: "x-driftmq-attempt",
  dlqSourceTopic: "x-driftmq-dlq-source-topic",
  dlqSourceOffset: "x-driftmq-dlq-source-offset",
  dlqConsumerId: "x-driftmq-dlq-consumer-id",
  dlqAttempts: "x-driftmq-dlq-attempts",
  dlqFailedAt: "x-driftmq-dlq-failed-at",
} as const;

export function isReservedHeaderKey(key: string): boolean {
  return key.toLowerCase().startsWith(RESERVED_HEADER_PREFIX);
}

export function headerText(
  headers: readonly WireHeader[],
  key: string,
): string | undefined {
  const lowered = key.toLowerCase();
  for (const header of headers) {
    if (header.key.toLowerCase() === lowered) return header.value.toString("utf8");
  }
  return undefined;
}

function headerNumber(headers: readonly WireHeader[], key: string): number | undefined {
  const text = headerText(headers, key);
  if (text === undefined) return undefined;
  const value = Number(text);
  return Number.isFinite(value) ? value : undefined;
}

/**
 * 이 토픽 안에서 이 메시지가 몇 번째로 전달됐는가. 브로커가 재전달할 때만 올라가며
 * 붙어 있지 않으면 1이다 — job의 attempt(envelope)와는 다른 값이다.
 */
export function readAttemptCount(headers: readonly WireHeader[]): number {
  const value = headerNumber(headers, DRIFTMQ_HEADERS.attempt);
  return value === undefined || value < 1 ? 1 : Math.floor(value);
}

/** 브로커 DLQ(`<topic>.dlq`) 레코드에만 붙는 메타데이터. */
export interface BrokerDeadLetterInfo {
  readonly sourceTopic: string;
  readonly sourceOffset: number | undefined;
  readonly consumerId: string | undefined;
  readonly attempts: number | undefined;
  readonly failedAt: number | undefined;
}

export function readBrokerDeadLetterInfo(
  headers: readonly WireHeader[],
): BrokerDeadLetterInfo | undefined {
  const sourceTopic = headerText(headers, DRIFTMQ_HEADERS.dlqSourceTopic);
  if (sourceTopic === undefined) return undefined;

  return {
    sourceTopic,
    sourceOffset: headerNumber(headers, DRIFTMQ_HEADERS.dlqSourceOffset),
    consumerId: headerText(headers, DRIFTMQ_HEADERS.dlqConsumerId),
    attempts: headerNumber(headers, DRIFTMQ_HEADERS.dlqAttempts),
    failedAt: headerNumber(headers, DRIFTMQ_HEADERS.dlqFailedAt),
  };
}
