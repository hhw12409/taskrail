import { TaskrailError } from "../support/errors.js";
import type { JobHandler, JobOptions } from "./definition.js";

export interface RegisteredJob {
  readonly name: string;
  // 등록 시점에 P가 확정되므로 저장할 때만 제네릭을 지운다.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly handler: JobHandler<any, unknown>;
  readonly options: JobOptions;
}

/** enqueuer도 registry를 갖는다 — 정의되지 않은 이름의 enqueue를 즉시 막기 위해서다. */
export class JobRegistry {
  readonly #jobs = new Map<string, RegisteredJob>();

  register(job: RegisteredJob): void {
    if (this.#jobs.has(job.name)) {
      throw new TaskrailError(`job "${job.name}"은 이미 정의되어 있다`);
    }
    this.#jobs.set(job.name, job);
  }

  get(name: string): RegisteredJob | undefined {
    return this.#jobs.get(name);
  }

  has(name: string): boolean {
    return this.#jobs.has(name);
  }

  names(): string[] {
    return [...this.#jobs.keys()];
  }
}
