interface Waiter {
  readonly grant: (granted: boolean) => void;
  release(): void;
}

/**
 * worker 안에서 동시에 RUNNING일 수 있는 job 수의 상한. prefetch가 concurrency보다 크면
 * 어댑터가 먼저 건네준 전달이 여기서 대기한다.
 */
export class ConcurrencyLimiter {
  #permits: number;
  readonly #waiters = new Set<Waiter>();

  constructor(permits: number) {
    this.#permits = permits;
  }

  get available(): number {
    return this.#permits;
  }

  get waiting(): number {
    return this.#waiters.size;
  }

  /** abort되면 슬롯을 주지 않고 false를 돌려준다 — 호출자가 전달을 반납해야 한다. */
  async acquire(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) return false;
    if (this.#permits > 0) {
      this.#permits -= 1;
      return true;
    }

    return new Promise<boolean>((resolve) => {
      const waiter: Waiter = {
        grant: resolve,
        release: () => signal.removeEventListener("abort", onAbort),
      };
      const onAbort = (): void => {
        this.#waiters.delete(waiter);
        resolve(false);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      this.#waiters.add(waiter);
    });
  }

  /** 대기자가 있으면 permit을 되돌리지 않고 그대로 넘긴다. */
  release(): void {
    const next = this.#waiters.values().next();
    if (next.done === true) {
      this.#permits += 1;
      return;
    }
    this.#waiters.delete(next.value);
    next.value.release();
    next.value.grant(true);
  }
}
