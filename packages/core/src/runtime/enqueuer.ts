import {
  DEFAULT_BACKOFF,
  assertSerializableBackoff,
} from "../job/backoff.js";
import type { BackoffPolicy } from "../job/backoff.js";
import {
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_QUEUE,
  DEFAULT_TIMEOUT_MS,
} from "../job/definition.js";
import type { EnqueueOptions, JobOptions } from "../job/definition.js";
import { ENVELOPE_VERSION } from "../queue/envelope.js";
import type { JobEnvelope } from "../queue/envelope.js";
import { TaskrailError } from "../support/errors.js";
import { ulid } from "../support/ulid.js";

export interface ResolvedJobOptions {
  readonly queue: string;
  readonly maxAttempts: number;
  readonly backoff: BackoffPolicy;
  readonly timeoutMs: number;
}

/** 나중에 오는 layer가 이긴다: Taskrail defaults < job() 옵션 < enqueue() 옵션. */
export function resolveJobOptions(
  ...layers: readonly (JobOptions | EnqueueOptions | undefined)[]
): ResolvedJobOptions {
  let queue: string = DEFAULT_QUEUE;
  let maxAttempts: number = DEFAULT_MAX_ATTEMPTS;
  let backoff: BackoffPolicy = DEFAULT_BACKOFF;
  let timeoutMs: number = DEFAULT_TIMEOUT_MS;

  for (const layer of layers) {
    if (layer === undefined) continue;
    if ("queue" in layer && layer.queue !== undefined) queue = layer.queue;
    if (layer.maxAttempts !== undefined) maxAttempts = layer.maxAttempts;
    if (layer.backoff !== undefined) backoff = layer.backoff;
    if (layer.timeoutMs !== undefined) timeoutMs = layer.timeoutMs;
  }

  return { queue, maxAttempts, backoff, timeoutMs };
}

/** delayMs / runAt을 단일 필드 notBefore로 정규화한다. 과거 시각은 에러가 아니다. */
export function resolveNotBefore(
  options: EnqueueOptions | undefined,
  now: number,
): number {
  const hasDelay = options?.delayMs !== undefined;
  const hasRunAt = options?.runAt !== undefined;

  if (hasDelay && hasRunAt) {
    throw new TaskrailError("delayMs와 runAt은 동시에 줄 수 없다");
  }
  if (hasDelay) {
    const delayMs = options!.delayMs!;
    if (!Number.isFinite(delayMs)) {
      throw new TaskrailError("delayMs는 유한한 숫자여야 한다");
    }
    return now + Math.max(0, delayMs);
  }
  if (hasRunAt) {
    const time = options!.runAt!.getTime();
    if (Number.isNaN(time)) {
      throw new TaskrailError("runAt이 유효한 Date가 아니다");
    }
    return time;
  }
  return now;
}

/** 직렬화 불가 payload는 큐에 넣지 않는다 — 나중에 poison message가 되는 것보다 낫다. */
export function assertJsonSerializable(payload: unknown): void {
  if (payload === undefined) return;
  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(payload);
  } catch (error) {
    throw new TaskrailError("payload를 JSON으로 직렬화할 수 없다", { cause: error });
  }
  if (encoded === undefined) {
    throw new TaskrailError("payload를 JSON으로 직렬화할 수 없다 (function/symbol)");
  }
}

export interface BuildEnvelopeInput {
  readonly name: string;
  readonly payload: unknown;
  readonly resolved: ResolvedJobOptions;
  readonly enqueueOptions?: EnqueueOptions | undefined;
  readonly now?: number;
}

export function buildEnvelope(input: BuildEnvelopeInput): JobEnvelope {
  const now = input.now ?? Date.now();
  const { maxAttempts, timeoutMs, queue, backoff } = input.resolved;

  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new TaskrailError("maxAttempts는 1 이상의 정수여야 한다");
  }
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new TaskrailError("timeoutMs는 0보다 큰 숫자여야 한다");
  }

  assertSerializableBackoff(backoff);
  assertJsonSerializable(input.payload);

  return {
    v: ENVELOPE_VERSION,
    jobId: input.enqueueOptions?.jobId ?? ulid(now),
    name: input.name,
    queue,
    payload: input.payload,
    attempt: 1,
    maxAttempts,
    enqueuedAt: now,
    notBefore: resolveNotBefore(input.enqueueOptions, now),
    timeoutMs,
    backoff,
  };
}
