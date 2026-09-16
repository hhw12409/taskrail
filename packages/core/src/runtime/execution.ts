import type { EventBus } from "../events/bus.js";
import { computeBackoffDelay } from "../job/backoff.js";
import type { JobContext } from "../job/definition.js";
import type { RegisteredJob } from "../job/registry.js";
import { nextAttemptEnvelope } from "../queue/envelope.js";
import type { JobEnvelope } from "../queue/envelope.js";
import type {
  DeadLetterReason,
  Delivery,
  DeliveryReceipt,
  JobQueue,
} from "../queue/queue.js";
import { TimeoutError, describeError, isFatalJobError } from "../support/errors.js";
import { withContext } from "../support/logger.js";
import type { Logger } from "../support/logger.js";
import type { JobStateRecorder } from "./state.js";

/** ack 실패는 job 결과를 바꾸지 않는다 — 재전달되면 중복으로 처리된다. */
export async function settleAck(
  queue: JobQueue,
  receipt: DeliveryReceipt,
  logger: Logger,
): Promise<void> {
  try {
    await queue.ack(receipt);
  } catch (error) {
    logger.error("ack 실패", { error });
  }
}

export async function settleNack(
  queue: JobQueue,
  receipt: DeliveryReceipt,
  logger: Logger,
): Promise<void> {
  try {
    await queue.nack(receipt, { requeue: true });
  } catch (error) {
    logger.error("nack 실패", { error });
  }
}

export interface ExecutorDeps {
  readonly queue: JobQueue;
  readonly events: EventBus;
  readonly state: JobStateRecorder;
  readonly logger: Logger;
  readonly workerId: string;
  readonly shutdownSignal: AbortSignal;
}

/** 전달 하나의 실행과 그 결과(성공/재시도/DEAD) 처리. 동시성 슬롯은 호출자가 잡는다. */
export class JobExecutor {
  readonly #deps: ExecutorDeps;

  constructor(deps: ExecutorDeps) {
    this.#deps = deps;
  }

  async run(delivery: Delivery, job: RegisteredJob): Promise<void> {
    const envelope = delivery.envelope;
    const startedAt = new Date();

    await this.#deps.state.record(envelope, "RUNNING", { startedAt });
    this.#deps.events.emit("job.started", {
      jobId: envelope.jobId,
      name: envelope.name,
      attempt: envelope.attempt,
      deliveryCount: delivery.deliveryCount,
    });

    const controller = new AbortController();
    const unlink = linkAbort(this.#deps.shutdownSignal, controller);
    const context = this.#context(delivery, controller.signal);

    try {
      await withTimeout(
        () => job.handler(envelope.payload, context),
        envelope.timeoutMs,
        controller,
      );
      await this.#succeed(delivery, Date.now() - startedAt.getTime());
    } catch (error) {
      await this.#fail(delivery, error);
    } finally {
      unlink();
    }
  }

  /** 실행하지 않고 끝내는 경로(알 수 없는 job 등). 실행이 없으므로 job.failed도 없다. */
  async discard(
    envelope: JobEnvelope,
    receipt: DeliveryReceipt,
    reason: DeadLetterReason,
  ): Promise<void> {
    if (!(await this.#toDead(envelope, receipt, reason))) return;
    await this.#deps.state.record(envelope, "DEAD", { finishedAt: new Date() });
    this.#deps.events.emit("job.dead", {
      jobId: envelope.jobId,
      name: envelope.name,
      attempt: envelope.attempt,
      reason,
    });
  }

  async #succeed(delivery: Delivery, durationMs: number): Promise<void> {
    const envelope = delivery.envelope;
    await settleAck(this.#deps.queue, delivery.receipt, this.#deps.logger);
    await this.#deps.state.record(envelope, "SUCCESS", { finishedAt: new Date() });
    this.#deps.events.emit("job.completed", {
      jobId: envelope.jobId,
      name: envelope.name,
      attempt: envelope.attempt,
      durationMs,
    });
  }

  async #fail(delivery: Delivery, error: unknown): Promise<void> {
    const envelope = delivery.envelope;
    const described = describeError(error);
    const fatal = isFatalJobError(error);

    this.#deps.events.emit("job.failed", {
      jobId: envelope.jobId,
      name: envelope.name,
      attempt: envelope.attempt,
      error,
      timedOut: error instanceof TimeoutError,
    });
    await this.#deps.state.record(envelope, "FAILED", { lastError: described });

    if (fatal || envelope.attempt >= envelope.maxAttempts) {
      const reason: DeadLetterReason = fatal
        ? { kind: "fatal", error: described.message }
        : {
            kind: "attempts-exhausted",
            attempt: envelope.attempt,
            error: described.message,
          };

      if (!(await this.#toDead(envelope, delivery.receipt, reason))) return;
      await this.#deps.state.record(envelope, "DEAD", {
        finishedAt: new Date(),
        lastError: described,
      });
      this.#deps.events.emit("job.dead", {
        jobId: envelope.jobId,
        name: envelope.name,
        attempt: envelope.attempt,
        reason,
      });
      return;
    }

    const delayMs = computeBackoffDelay(envelope.backoff, envelope.attempt, error);
    const next = nextAttemptEnvelope(envelope, Date.now() + delayMs);

    try {
      await this.#deps.queue.enqueue(next);
    } catch (enqueueError) {
      // 원본을 ack하지 않는다 — lease 만료 재전달이 같은 attempt를 되살린다.
      this.#deps.logger.error("재시도 enqueue 실패", {
        jobId: envelope.jobId,
        attempt: next.attempt,
        error: enqueueError,
      });
      return;
    }

    await settleAck(this.#deps.queue, delivery.receipt, this.#deps.logger);
    await this.#deps.state.record(envelope, "RETRY_WAIT", {
      attempt: next.attempt,
      lastError: described,
    });
    this.#deps.events.emit("job.retried", {
      jobId: envelope.jobId,
      name: envelope.name,
      nextAttempt: next.attempt,
      delayMs,
    });
  }

  /** dead 이동이 실패하면 ack하지 않는다 — 메시지를 잃는 것보다 재전달이 낫다. */
  async #toDead(
    envelope: JobEnvelope,
    receipt: DeliveryReceipt,
    reason: DeadLetterReason,
  ): Promise<boolean> {
    try {
      await this.#deps.queue.deadLetter(envelope, reason);
    } catch (error) {
      this.#deps.logger.error("deadLetter 실패", { jobId: envelope.jobId, error });
      return false;
    }
    await settleAck(this.#deps.queue, receipt, this.#deps.logger);
    return true;
  }

  #context(delivery: Delivery, signal: AbortSignal): JobContext {
    const envelope = delivery.envelope;
    return {
      jobId: envelope.jobId,
      name: envelope.name,
      queue: envelope.queue,
      attempt: envelope.attempt,
      maxAttempts: envelope.maxAttempts,
      deliveryCount: delivery.deliveryCount,
      enqueuedAt: new Date(envelope.enqueuedAt),
      signal,
      logger: withContext(this.#deps.logger, {
        workerId: this.#deps.workerId,
        jobId: envelope.jobId,
        name: envelope.name,
        attempt: envelope.attempt,
      }),
    };
  }
}

/** handler를 강제로 중단하지는 못한다. 슬롯을 회수하고 실패로 판정할 뿐이다. */
async function withTimeout<T>(
  run: () => Promise<T>,
  timeoutMs: number,
  controller: AbortController,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;

  const expiry = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      const error = new TimeoutError(timeoutMs);
      controller.abort(error);
      reject(error);
    }, timeoutMs);
  });

  try {
    return await Promise.race([Promise.resolve().then(run), expiry]);
  } finally {
    clearTimeout(timer);
  }
}

function linkAbort(source: AbortSignal, target: AbortController): () => void {
  if (source.aborted) {
    target.abort(source.reason);
    return () => {};
  }
  const onAbort = (): void => target.abort(source.reason);
  source.addEventListener("abort", onAbort, { once: true });
  return () => source.removeEventListener("abort", onAbort);
}
