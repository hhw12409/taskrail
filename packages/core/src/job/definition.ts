import type { BackoffPolicy } from "./backoff.js";
import type { Logger } from "../support/logger.js";

export type JobHandler<P, R = void> = (payload: P, ctx: JobContext) => Promise<R>;

export interface JobDefinition<P> {
  readonly name: string;
  enqueue(payload: P, options?: EnqueueOptions): Promise<JobHandle>;
}

export interface JobHandle {
  readonly jobId: string;
}

export interface JobOptions {
  queue?: string;
  maxAttempts?: number;
  backoff?: BackoffPolicy;
  timeoutMs?: number;
}

export interface EnqueueOptions {
  /** delayMs와 runAt은 동시에 줄 수 없다. */
  delayMs?: number;
  runAt?: Date;
  /** 중복 enqueue 억제에는 쓰이지 않는다 — 모든 attempt에서 ctx.jobId로 쓰일 뿐이다. */
  jobId?: string;
  maxAttempts?: number;
  backoff?: BackoffPolicy;
  timeoutMs?: number;
}

export interface JobContext {
  /** 재시도/재전달에도 변하지 않는다 → idempotency key. */
  readonly jobId: string;
  readonly name: string;
  readonly queue: string;
  /** 1부터. 브로커 재전달은 증가시키지 않는다. */
  readonly attempt: number;
  readonly maxAttempts: number;
  /** 1보다 크면 재전달이 일어난 것. */
  readonly deliveryCount: number;
  readonly enqueuedAt: Date;
  /** timeout 또는 graceful shutdown 시 abort된다. 외부 호출에 전달할 것. */
  readonly signal: AbortSignal;
  readonly logger: Logger;
}

export const DEFAULT_QUEUE = "default";
export const DEFAULT_MAX_ATTEMPTS = 3;
export const DEFAULT_TIMEOUT_MS = 30_000;
