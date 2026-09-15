import type { EventBus } from "../events/bus.js";
import { DEFAULT_QUEUE } from "../job/definition.js";
import type { JobRegistry } from "../job/registry.js";
import type { JobStateStore } from "../job/snapshot.js";
import type { JobQueue } from "../queue/queue.js";
import { TaskrailError } from "../support/errors.js";
import type { Logger } from "../support/logger.js";
import { ulid } from "../support/ulid.js";

export interface WorkerOptions {
  queues?: string[];
  concurrency?: number;
  prefetch?: number;
  /** nativeDelay 없는 어댑터용. */
  inlineDelayMs?: number;
  requeueIntervalMs?: number;
}

export interface Worker {
  readonly id: string;
  start(): Promise<void>;
  /** 멱등. 두 번째 호출은 첫 번째 완료를 기다린다. */
  stop(options?: { timeoutMs?: number }): Promise<void>;
  readonly inFlight: number;
}

export interface ResolvedWorkerOptions {
  readonly queues: readonly string[];
  readonly concurrency: number;
  readonly prefetch: number;
  readonly inlineDelayMs: number;
  readonly requeueIntervalMs: number;
}

export const DEFAULT_CONCURRENCY = 10;
export const DEFAULT_INLINE_DELAY_MS = 5_000;
export const DEFAULT_REQUEUE_INTERVAL_MS = 30_000;
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;

/** prefetch 기본값을 concurrency로 묶는 것이 backpressure 계약이다. */
export function resolveWorkerOptions(
  options: WorkerOptions | undefined,
): ResolvedWorkerOptions {
  const queues = options?.queues ?? [DEFAULT_QUEUE];
  if (queues.length === 0) {
    throw new TaskrailError("worker의 queues는 비어 있을 수 없다");
  }

  const concurrency = options?.concurrency ?? DEFAULT_CONCURRENCY;
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new TaskrailError("concurrency는 1 이상의 정수여야 한다");
  }

  const prefetch = options?.prefetch ?? concurrency;
  if (!Number.isInteger(prefetch) || prefetch < 1) {
    throw new TaskrailError("prefetch는 1 이상의 정수여야 한다");
  }

  return {
    queues,
    concurrency,
    prefetch,
    inlineDelayMs: options?.inlineDelayMs ?? DEFAULT_INLINE_DELAY_MS,
    requeueIntervalMs: options?.requeueIntervalMs ?? DEFAULT_REQUEUE_INTERVAL_MS,
  };
}

export interface WorkerDeps {
  readonly queue: JobQueue;
  readonly registry: JobRegistry;
  readonly events: EventBus;
  readonly stateStore: JobStateStore;
  readonly logger: Logger;
  readonly options: ResolvedWorkerOptions;
}

type WorkerPhase = "created" | "running" | "stopping" | "stopped";

/**
 * 골격만 있다. ConsumeLoop / ConcurrencyLimiter / TimeoutGuard / RetryPolicy /
 * ShutdownController는 3단계(Job runtime)에서 채운다.
 */
export class WorkerImpl implements Worker {
  readonly id: string;
  readonly #deps: WorkerDeps;
  readonly #shutdown = new AbortController();
  #phase: WorkerPhase = "created";
  #inFlight = 0;
  #stopPromise: Promise<void> | undefined;

  constructor(deps: WorkerDeps) {
    this.#deps = deps;
    this.id = `worker-${ulid()}`;
  }

  get inFlight(): number {
    return this.#inFlight;
  }

  get options(): ResolvedWorkerOptions {
    return this.#deps.options;
  }

  get phase(): WorkerPhase {
    return this.#phase;
  }

  get shutdownSignal(): AbortSignal {
    return this.#shutdown.signal;
  }

  async start(): Promise<void> {
    if (this.#phase !== "created") {
      throw new TaskrailError(`worker ${this.id}는 이미 start()되었다`);
    }
    this.#phase = "running";
    // TODO(3단계): queue.consume(handler, { queue, prefetch, signal: shutdownSignal })
    throw new TaskrailError(
      "Worker 실행 루프는 아직 구현되지 않았다 (3단계: Job runtime).",
    );
  }

  async stop(options?: { timeoutMs?: number }): Promise<void> {
    if (this.#stopPromise !== undefined) return this.#stopPromise;
    this.#stopPromise = this.#doStop(
      options?.timeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS,
    );
    return this.#stopPromise;
  }

  async #doStop(_timeoutMs: number): Promise<void> {
    if (this.#phase === "stopped") return;
    this.#phase = "stopping";
    this.#shutdown.abort();
    // TODO(3단계): drain 절차 — consume 중지 → 미시작 전달 nack(requeue) →
    //   in-flight를 timeoutMs까지 대기 → subscription close
    this.#phase = "stopped";
  }
}
