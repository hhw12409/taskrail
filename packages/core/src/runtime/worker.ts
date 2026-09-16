import type { EventBus } from "../events/bus.js";
import { DEFAULT_QUEUE, DEFAULT_TIMEOUT_MS } from "../job/definition.js";
import type { JobOptions } from "../job/definition.js";
import type { JobRegistry } from "../job/registry.js";
import type { JobStateStore } from "../job/snapshot.js";
import type { Delivery, JobQueue, Subscription } from "../queue/queue.js";
import { TaskrailError } from "../support/errors.js";
import type { Logger } from "../support/logger.js";
import { ulid } from "../support/ulid.js";
import { RUN_NOW, decideDelay, sleep } from "./delay.js";
import { JobExecutor, settleAck, settleNack } from "./execution.js";
import { ConcurrencyLimiter } from "./limiter.js";
import { JobStateRecorder } from "./state.js";

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
  /** Taskrail 기본값. lease 경고에서 실효 timeoutMs를 계산할 때만 쓴다. */
  readonly defaults?: JobOptions | undefined;
}

type WorkerPhase = "created" | "running" | "stopping" | "stopped";

export class WorkerImpl implements Worker {
  readonly id: string;
  readonly #deps: WorkerDeps;
  readonly #shutdown = new AbortController();
  readonly #limiter: ConcurrencyLimiter;
  readonly #state: JobStateRecorder;
  readonly #executor: JobExecutor;
  readonly #subscriptions: Subscription[] = [];
  /** drain 대상. 어댑터에 건네준 handler의 완료 promise를 그대로 들고 있는다. */
  readonly #tasks = new Set<Promise<void>>();
  #phase: WorkerPhase = "created";
  #inFlight = 0;
  #stopPromise: Promise<void> | undefined;

  constructor(deps: WorkerDeps) {
    this.#deps = deps;
    this.id = `worker-${ulid()}`;
    this.#limiter = new ConcurrencyLimiter(deps.options.concurrency);
    this.#state = new JobStateRecorder(deps.stateStore, deps.logger);
    this.#executor = new JobExecutor({
      queue: deps.queue,
      events: deps.events,
      state: this.#state,
      logger: deps.logger,
      workerId: this.id,
      shutdownSignal: this.#shutdown.signal,
    });
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
    this.#warnOnTimeoutOverrun();

    try {
      for (const queue of this.#deps.options.queues) {
        const subscription = await this.#deps.queue.consume(
          (delivery) => this.#accept(delivery),
          {
            queue,
            prefetch: this.#deps.options.prefetch,
            signal: this.#shutdown.signal,
          },
        );
        this.#subscriptions.push(subscription);
      }
    } catch (error) {
      await this.stop({ timeoutMs: 0 });
      throw error;
    }

    this.#deps.logger.info("worker 시작", {
      workerId: this.id,
      queues: this.#deps.options.queues,
      concurrency: this.#deps.options.concurrency,
    });
  }

  async stop(options?: { timeoutMs?: number }): Promise<void> {
    if (this.#stopPromise !== undefined) return this.#stopPromise;
    this.#stopPromise = this.#doStop(
      options?.timeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS,
    );
    return this.#stopPromise;
  }

  async #doStop(timeoutMs: number): Promise<void> {
    if (this.#phase === "stopped") return;
    this.#phase = "stopping";

    // 새 전달을 끊고, 슬롯을 기다리던 전달을 풀어 주고, 실행 중 handler의 ctx.signal을 abort한다.
    this.#shutdown.abort();

    for (const subscription of this.#subscriptions) {
      try {
        await subscription.close();
      } catch (error) {
        this.#deps.logger.error("subscription close 실패", {
          workerId: this.id,
          error,
        });
      }
    }
    this.#subscriptions.length = 0;

    await this.#drain(timeoutMs);
    this.#phase = "stopped";
  }

  /** 기한을 넘긴 in-flight는 ack하지 않고 둔다 — 브로커 lease가 만료되면 재전달된다. */
  async #drain(timeoutMs: number): Promise<void> {
    const pending = [...this.#tasks];
    if (pending.length === 0) return;

    let timer: ReturnType<typeof setTimeout> | undefined;
    const expiry = new Promise<"expired">((resolve) => {
      timer = setTimeout(() => resolve("expired"), Math.max(0, timeoutMs));
    });

    try {
      const outcome = await Promise.race([
        Promise.all(pending).then(() => "drained" as const),
        expiry,
      ]);
      if (outcome === "expired") {
        this.#deps.logger.warn("shutdown 기한 내에 끝나지 않은 job을 남기고 종료한다", {
          workerId: this.id,
          remaining: this.#tasks.size,
        });
      }
    } finally {
      clearTimeout(timer);
    }
  }

  #accept(delivery: Delivery): Promise<void> {
    const task = this.#process(delivery);
    this.#tasks.add(task);
    void task.finally(() => {
      this.#tasks.delete(task);
    });
    return task;
  }

  /** 절대 reject하지 않는다 — 어댑터의 전달 루프를 망가뜨리지 않기 위해서다. */
  async #process(delivery: Delivery): Promise<void> {
    const envelope = delivery.envelope;
    try {
      const job = this.#deps.registry.get(envelope.name);
      if (job === undefined) {
        await this.#executor.discard(envelope, delivery.receipt, {
          kind: "unknown-job",
          name: envelope.name,
        });
        return;
      }

      await this.#state.record(envelope, "WAITING");

      const decision = this.#deps.queue.capabilities.nativeDelay
        ? RUN_NOW
        : decideDelay(envelope.notBefore, Date.now(), this.#deps.options);

      if (decision.kind === "requeue") {
        await this.#requeueLater(delivery, decision.ms);
        return;
      }

      if (!(await this.#limiter.acquire(this.#shutdown.signal))) {
        await settleNack(this.#deps.queue, delivery.receipt, this.#deps.logger);
        return;
      }

      this.#enter();
      try {
        if (decision.kind === "sleep") {
          if (!(await sleep(decision.ms, this.#shutdown.signal))) {
            await settleNack(this.#deps.queue, delivery.receipt, this.#deps.logger);
            return;
          }
        }
        await this.#executor.run(delivery, job);
      } finally {
        this.#leave();
        this.#limiter.release();
      }
    } catch (error) {
      this.#deps.logger.error("전달 처리 중 예외", {
        workerId: this.id,
        jobId: envelope.jobId,
        error,
      });
    }
  }

  /** 아직 실행할 때가 아니다. 슬롯을 점유하지 않도록 같은 attempt로 다시 큐에 넣는다. */
  async #requeueLater(delivery: Delivery, waitMs: number): Promise<void> {
    if (!(await sleep(waitMs, this.#shutdown.signal))) {
      await settleNack(this.#deps.queue, delivery.receipt, this.#deps.logger);
      return;
    }

    try {
      await this.#deps.queue.enqueue(delivery.envelope);
    } catch (error) {
      this.#deps.logger.error("지연 재enqueue 실패", {
        workerId: this.id,
        jobId: delivery.envelope.jobId,
        error,
      });
      return;
    }
    await settleAck(this.#deps.queue, delivery.receipt, this.#deps.logger);
  }

  #enter(): void {
    this.#inFlight += 1;
    if (this.#inFlight === 1) {
      this.#deps.events.emit("worker.active", {
        workerId: this.id,
        inFlight: this.#inFlight,
      });
    }
  }

  #leave(): void {
    this.#inFlight = Math.max(0, this.#inFlight - 1);
    if (this.#inFlight === 0) {
      this.#deps.events.emit("worker.idle", { workerId: this.id });
    }
  }

  /**
   * job timeout이 브로커 lease보다 길면 Taskrail 재시도와 브로커 재전달이 겹쳐 중복이 두 배가 된다.
   * 브로커 설정을 항상 정확히 알 수는 없으므로 경고만 남긴다.
   */
  #warnOnTimeoutOverrun(): void {
    const lease = this.#deps.queue.capabilities.visibilityTimeoutMs;
    if (lease === null) return;

    const fallback = this.#deps.defaults?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    for (const name of this.#deps.registry.names()) {
      const timeoutMs = this.#deps.registry.get(name)?.options.timeoutMs ?? fallback;
      if (timeoutMs >= lease) {
        this.#deps.logger.warn("job timeoutMs가 브로커 visibility timeout 이상이다", {
          workerId: this.id,
          name,
          timeoutMs,
          visibilityTimeoutMs: lease,
        });
      }
    }
  }
}
