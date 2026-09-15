import type { BackoffPolicy } from "../job/backoff.js";

export const ENVELOPE_VERSION = 1 as const;

/**
 * 브로커에 실제로 저장되는 형태. 언어 중립 JSON이며, 실행에 필요한 모든 상태가 여기 있다 —
 * Taskrail 프로세스가 전부 죽어도 attempt가 정확한 이유다.
 */
export interface JobEnvelope {
  readonly v: 1;
  readonly jobId: string;
  readonly name: string;
  readonly queue: string;
  readonly payload: unknown;
  readonly attempt: number;
  readonly maxAttempts: number;
  /** 최초 enqueue 시각. 재시도해도 불변. */
  readonly enqueuedAt: number;
  /** 지연/백오프의 단일 표현 (epoch ms). */
  readonly notBefore: number;
  readonly timeoutMs: number;
  /** type: "custom"은 직렬화 불가 → enqueue 시 거부된다. */
  readonly backoff: BackoffPolicy;
  /** 예약 필드. MVP에서는 채우지 않는다. */
  readonly traceparent?: string;
}

/** 실패 시 실행하지 말고 `undecodable`로 DEAD 처리해야 한다 — poison message 방지. */
export function isJobEnvelope(value: unknown): value is JobEnvelope {
  if (typeof value !== "object" || value === null) return false;
  const e = value as Record<string, unknown>;
  return (
    e["v"] === ENVELOPE_VERSION &&
    typeof e["jobId"] === "string" &&
    typeof e["name"] === "string" &&
    typeof e["queue"] === "string" &&
    typeof e["attempt"] === "number" &&
    typeof e["maxAttempts"] === "number" &&
    typeof e["enqueuedAt"] === "number" &&
    typeof e["notBefore"] === "number" &&
    typeof e["timeoutMs"] === "number" &&
    typeof e["backoff"] === "object" &&
    e["backoff"] !== null
  );
}

export function nextAttemptEnvelope(
  envelope: JobEnvelope,
  notBefore: number,
): JobEnvelope {
  return { ...envelope, attempt: envelope.attempt + 1, notBefore };
}
