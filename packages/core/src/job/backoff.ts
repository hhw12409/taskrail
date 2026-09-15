import { TaskrailError } from "../support/errors.js";

export type Jitter = "none" | "full" | "equal";

export type BackoffPolicy =
  | { type: "fixed"; delayMs: number; jitter?: Jitter }
  | {
      type: "exponential";
      delayMs: number;
      factor?: number;
      maxDelayMs?: number;
      jitter?: Jitter;
    }
  | { type: "custom"; compute: (attempt: number, error: unknown) => number };

export const DEFAULT_BACKOFF: BackoffPolicy = {
  type: "exponential",
  delayMs: 1_000,
  factor: 2,
  maxDelayMs: 60_000,
  jitter: "full",
};

export type SerializableBackoffPolicy = Exclude<BackoffPolicy, { type: "custom" }>;

export function isSerializableBackoff(
  policy: BackoffPolicy,
): policy is SerializableBackoffPolicy {
  return policy.type !== "custom";
}

/** backoff는 envelope에 실려 브로커에 저장되므로 함수는 담을 수 없다. */
export function assertSerializableBackoff(
  policy: BackoffPolicy,
): SerializableBackoffPolicy {
  if (!isSerializableBackoff(policy)) {
    throw new TaskrailError(
      'backoff { type: "custom" }은 직렬화할 수 없어 enqueue할 수 없다',
    );
  }
  return policy;
}

function applyJitter(base: number, jitter: Jitter, random: () => number): number {
  switch (jitter) {
    case "none":
      return base;
    case "full":
      return random() * base;
    case "equal":
      return base / 2 + random() * (base / 2);
  }
}

/** `attempt`는 방금 실패한 시도 번호(1부터)다. base = min(delayMs * factor^(attempt-1), maxDelayMs). */
export function computeBackoffDelay(
  policy: BackoffPolicy,
  attempt: number,
  error: unknown,
  random: () => number = Math.random,
): number {
  if (policy.type === "custom") {
    return Math.max(0, policy.compute(attempt, error));
  }

  const jitter: Jitter = policy.jitter ?? "none";

  if (policy.type === "fixed") {
    return Math.max(0, applyJitter(policy.delayMs, jitter, random));
  }

  const factor = policy.factor ?? 2;
  const maxDelayMs = policy.maxDelayMs ?? Number.POSITIVE_INFINITY;
  const base = Math.min(policy.delayMs * Math.pow(factor, attempt - 1), maxDelayMs);
  return Math.max(0, applyJitter(base, jitter, random));
}
