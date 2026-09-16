import { DriftmqError } from "../support/errors.js";

const VALID_TOPIC = /^[A-Za-z0-9._-]{1,255}$/;

/**
 * 브로커가 거부하는 토픽 이름은 MALFORMED_FRAME으로 돌아오고 연결까지 끊긴다.
 * 클라이언트에서 먼저 막는 편이 훨씬 싸다.
 */
export function isValidTopic(name: string): boolean {
  return VALID_TOPIC.test(name) && name !== "." && name !== "..";
}

export function assertValidTopic(name: string, what = "토픽 이름"): void {
  if (!isValidTopic(name)) {
    throw new DriftmqError(
      `${what}이 driftmq 규칙에 맞지 않는다: "${name}" (허용: [A-Za-z0-9._-] 1~255자, "." ".." 제외)`,
    );
  }
}
