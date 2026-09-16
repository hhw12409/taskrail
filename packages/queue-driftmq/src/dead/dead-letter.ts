import type { DeadLetterReason } from "@taskrail/core";

import { isReservedHeaderKey } from "../message/reserved.js";
import type { WireHeader } from "../wire/types.js";

/** dead 토픽 레코드에만 붙는 헤더. 예약 접두사와 겹치지 않는 이름이어야 한다. */
export const DEAD_HEADERS = {
  reason: "dead-reason",
  error: "dead-error",
  at: "dead-at",
} as const;

export function deadHeaderValues(
  reason: DeadLetterReason,
  at: number,
): Record<string, string> {
  return {
    [DEAD_HEADERS.reason]: reason.kind,
    [DEAD_HEADERS.error]: describeReason(reason),
    [DEAD_HEADERS.at]: String(at),
  };
}

function describeReason(reason: DeadLetterReason): string {
  switch (reason.kind) {
    case "attempts-exhausted":
      return `attempt ${reason.attempt}: ${reason.error}`;
    case "fatal":
      return reason.error;
    case "unknown-job":
      return `unknown job "${reason.name}"`;
    case "undecodable":
      return reason.detail;
  }
}

/**
 * 원본 헤더를 그대로 다시 publish하면 브로커가 붙인 예약 헤더 때문에 거부당한다.
 * 사용자 헤더만 남겨 보존한다.
 */
export function withoutReservedHeaders(
  headers: readonly WireHeader[],
): readonly WireHeader[] {
  return headers.filter((header) => !isReservedHeaderKey(header.key));
}
