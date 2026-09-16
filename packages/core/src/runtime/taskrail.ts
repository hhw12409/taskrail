import { EventBus } from "../events/bus.js";
import type { TaskrailEvents } from "../events/types.js";
import type {
  EnqueueOptions,
  JobDefinition,
  JobHandle,
  JobHandler,
  JobOptions,
} from "../job/definition.js";
import { JobRegistry } from "../job/registry.js";
import { MemoryStateStore } from "../job/snapshot.js";
import type { JobSnapshot, JobStateStore } from "../job/snapshot.js";
import type { JobQueue } from "../queue/queue.js";
import { TaskrailError } from "../support/errors.js";
import { noopLogger } from "../support/logger.js";
import type { Logger } from "../support/logger.js";
import { buildEnvelope, resolveJobOptions } from "./enqueuer.js";
import { WorkerImpl, resolveWorkerOptions } from "./worker.js";
import type { Worker, WorkerOptions } from "./worker.js";

export interface TaskrailOptions {
  queue: JobQueue;
  /** 기본 MemoryStateStore — 프로세스 로컬, 비영속. */
  stateStore?: JobStateStore;
  defaults?: JobOptions;
  logger?: Logger;
}

export class Taskrail {
  readonly #queue: JobQueue;
  readonly #stateStore: JobStateStore;
  readonly #defaults: JobOptions | undefined;
  readonly #logger: Logger;
  readonly #registry = new JobRegistry();
  readonly #events: EventBus;
  #closed = false;

  constructor(options: TaskrailOptions) {
    if (options.queue === undefined || options.queue === null) {
      throw new TaskrailError("TaskrailOptions.queue는 필수다");
    }
    this.#queue = options.queue;
    this.#stateStore = options.stateStore ?? new MemoryStateStore();
    this.#defaults = options.defaults;
    this.#logger = options.logger ?? noopLogger;
    this.#events = new EventBus(this.#logger);
  }

  job<P = unknown, R = void>(
    name: string,
    handler: JobHandler<P, R>,
    options?: JobOptions,
  ): JobDefinition<P> {
    if (typeof name !== "string" || name.length === 0) {
      throw new TaskrailError("job 이름은 비어 있지 않은 문자열이어야 한다");
    }
    this.#registry.register({
      name,
      handler: handler as JobHandler<unknown, unknown>,
      options: options ?? {},
    });

    return {
      name,
      enqueue: (payload: P, enqueueOptions?: EnqueueOptions) =>
        this.#enqueue(name, payload, options, enqueueOptions),
    };
  }

  createWorker(options?: WorkerOptions): Worker {
    this.#assertOpen();
    return new WorkerImpl({
      queue: this.#queue,
      registry: this.#registry,
      events: this.#events,
      stateStore: this.#stateStore,
      logger: this.#logger,
      options: resolveWorkerOptions(options),
      defaults: this.#defaults,
    });
  }

  async getState(jobId: string): Promise<JobSnapshot | undefined> {
    return this.#stateStore.get(jobId);
  }

  on<E extends keyof TaskrailEvents>(event: E, listener: TaskrailEvents[E]): this {
    this.#events.on(event, listener);
    return this;
  }

  off<E extends keyof TaskrailEvents>(event: E, listener: TaskrailEvents[E]): this {
    this.#events.off(event, listener);
    return this;
  }

  /** worker는 각자 stop()해야 한다 — close()는 큐 연결만 닫는다. */
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#queue.close();
  }

  async #enqueue(
    name: string,
    payload: unknown,
    jobOptions: JobOptions | undefined,
    enqueueOptions: EnqueueOptions | undefined,
  ): Promise<JobHandle> {
    this.#assertOpen();

    if (!this.#registry.has(name)) {
      throw new TaskrailError(`정의되지 않은 job "${name}"을 enqueue할 수 없다`);
    }

    const envelope = buildEnvelope({
      name,
      payload,
      resolved: resolveJobOptions(this.#defaults, jobOptions, enqueueOptions),
      enqueueOptions,
    });

    // 실패를 삼키지 않는다 — 호출자의 트랜잭션 안에서 다뤄야 한다.
    await this.#queue.enqueue(envelope);

    await this.#record({
      jobId: envelope.jobId,
      name: envelope.name,
      queue: envelope.queue,
      status: "WAITING",
      attempt: envelope.attempt,
      maxAttempts: envelope.maxAttempts,
      enqueuedAt: new Date(envelope.enqueuedAt),
    });

    this.#events.emit("job.enqueued", {
      jobId: envelope.jobId,
      name: envelope.name,
      queue: envelope.queue,
      notBefore: envelope.notBefore,
      attempt: envelope.attempt,
    });

    return { jobId: envelope.jobId };
  }

  /** state store는 관측용이므로 실패해도 job 경로를 막지 않는다. */
  async #record(snapshot: JobSnapshot): Promise<void> {
    try {
      await this.#stateStore.record(snapshot);
    } catch (error) {
      this.#logger.error("stateStore.record 실패", {
        jobId: snapshot.jobId,
        error,
      });
    }
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new TaskrailError("Taskrail이 이미 close()되었다");
    }
  }
}
