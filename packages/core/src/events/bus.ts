import type { Logger } from "../support/logger.js";
import type {
  TaskrailEventName,
  TaskrailEventPayload,
  TaskrailEvents,
} from "./types.js";

export class EventBus {
  readonly #listeners = new Map<TaskrailEventName, Set<(e: never) => void>>();
  readonly #logger: Logger;

  constructor(logger: Logger) {
    this.#logger = logger;
  }

  on<E extends TaskrailEventName>(event: E, listener: TaskrailEvents[E]): void {
    let set = this.#listeners.get(event);
    if (set === undefined) {
      set = new Set();
      this.#listeners.set(event, set);
    }
    set.add(listener as (e: never) => void);
  }

  off<E extends TaskrailEventName>(event: E, listener: TaskrailEvents[E]): void {
    this.#listeners.get(event)?.delete(listener as (e: never) => void);
  }

  /** 리스너의 예외는 잡아서 로그한다 — job 실행에 영향을 주지 않는다. */
  emit<E extends TaskrailEventName>(event: E, payload: TaskrailEventPayload<E>): void {
    const set = this.#listeners.get(event);
    if (set === undefined) return;
    for (const listener of set) {
      try {
        (listener as (e: TaskrailEventPayload<E>) => void)(payload);
      } catch (error) {
        this.#logger.error("taskrail event listener threw", { event, error });
      }
    }
  }

  removeAll(): void {
    this.#listeners.clear();
  }
}
