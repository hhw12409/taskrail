import type { JobStatus } from "./status.js";

/** 관측용. 실행에 필요한 상태는 envelope에 실린다. */
export interface JobSnapshot {
  readonly jobId: string;
  readonly name: string;
  readonly queue: string;
  readonly status: JobStatus;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly enqueuedAt: Date;
  readonly startedAt?: Date;
  readonly finishedAt?: Date;
  readonly lastError?: { message: string; stack?: string };
}

export interface JobStateStore {
  record(snapshot: JobSnapshot): Promise<void>;
  get(jobId: string): Promise<JobSnapshot | undefined>;
}

/** 프로세스 로컬·비영속. 내구성 있는 job history는 MVP Non-goal이다. */
export class MemoryStateStore implements JobStateStore {
  readonly #snapshots = new Map<string, JobSnapshot>();

  async record(snapshot: JobSnapshot): Promise<void> {
    this.#snapshots.set(snapshot.jobId, snapshot);
  }

  async get(jobId: string): Promise<JobSnapshot | undefined> {
    return this.#snapshots.get(jobId);
  }

  get size(): number {
    return this.#snapshots.size;
  }

  clear(): void {
    this.#snapshots.clear();
  }
}
