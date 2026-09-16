import type { JobSnapshot, JobStateStore } from "../job/snapshot.js";
import { isTerminal } from "../job/status.js";
import type { JobStatus } from "../job/status.js";
import type { JobEnvelope } from "../queue/envelope.js";
import type { Logger } from "../support/logger.js";

export interface StateDetails {
  /** 재시도 기록처럼 envelope의 attempt와 달라야 할 때만 준다. */
  readonly attempt?: number;
  readonly startedAt?: Date;
  readonly finishedAt?: Date;
  readonly lastError?: { message: string; stack?: string };
}

/**
 * 상태 기록은 관측용이다 — 실패해도 job 실행 경로를 막지 않는다.
 * 종결 상태(SUCCESS/DEAD)는 덮어쓰지 않는다: 중복 전달이 이미 끝난 job의 기록을 되돌리면 안 된다.
 */
export class JobStateRecorder {
  readonly #store: JobStateStore;
  readonly #logger: Logger;

  constructor(store: JobStateStore, logger: Logger) {
    this.#store = store;
    this.#logger = logger;
  }

  async record(
    envelope: JobEnvelope,
    status: JobStatus,
    details?: StateDetails,
  ): Promise<void> {
    try {
      const previous = await this.#store.get(envelope.jobId);
      if (previous !== undefined && isTerminal(previous.status)) return;

      const startedAt = details?.startedAt ?? previous?.startedAt;
      const lastError = details?.lastError ?? previous?.lastError;

      const snapshot: JobSnapshot = {
        jobId: envelope.jobId,
        name: envelope.name,
        queue: envelope.queue,
        status,
        attempt: details?.attempt ?? envelope.attempt,
        maxAttempts: envelope.maxAttempts,
        enqueuedAt: new Date(envelope.enqueuedAt),
        ...(startedAt !== undefined ? { startedAt } : {}),
        ...(details?.finishedAt !== undefined ? { finishedAt: details.finishedAt } : {}),
        ...(lastError !== undefined ? { lastError } : {}),
      };

      await this.#store.record(snapshot);
    } catch (error) {
      this.#logger.error("stateStore 갱신 실패", {
        jobId: envelope.jobId,
        status,
        error,
      });
    }
  }
}
