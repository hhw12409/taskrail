import type { DeadLetterReason, JobEnvelope } from "@taskrail/core";

export interface DeadLetterRecord {
  readonly envelope: JobEnvelope;
  readonly reason: DeadLetterReason;
  readonly deadLetteredAt: number;
}

/**
 * `taskrail.<queue>.dead`에 해당하는 메모리 목적지. 브로커 DLQ가 아니라
 * Taskrail이 "더 시도하지 않는다"고 판단한 job이 쌓이는 곳이다.
 */
export class DeadLetterStore {
  readonly #records: DeadLetterRecord[] = [];

  get size(): number {
    return this.#records.length;
  }

  add(envelope: JobEnvelope, reason: DeadLetterReason, at: number): void {
    this.#records.push({ envelope, reason, deadLetteredAt: at });
  }

  list(): readonly DeadLetterRecord[] {
    return this.#records;
  }

  find(jobId: string): DeadLetterRecord | undefined {
    return this.#records.find((record) => record.envelope.jobId === jobId);
  }

  clear(): void {
    this.#records.length = 0;
  }
}
