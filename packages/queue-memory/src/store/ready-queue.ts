import type { QueuedMessage } from "./message.js";

function precedes(a: QueuedMessage, b: QueuedMessage): boolean {
  return a.readyAt !== b.readyAt ? a.readyAt < b.readyAt : a.id < b.id;
}

/** `readyAt` 기준 min-heap. 같은 시각이면 먼저 들어온 메시지가 먼저 나간다. */
export class ReadyQueue {
  readonly #heap: QueuedMessage[] = [];

  get size(): number {
    return this.#heap.length;
  }

  peek(): QueuedMessage | undefined {
    return this.#heap[0];
  }

  push(message: QueuedMessage): void {
    this.#heap.push(message);
    this.#siftUp(this.#heap.length - 1);
  }

  pop(): QueuedMessage | undefined {
    const top = this.#heap[0];
    if (top === undefined) return undefined;

    const last = this.#heap.pop() as QueuedMessage;
    if (this.#heap.length > 0) {
      this.#heap[0] = last;
      this.#siftDown(0);
    }
    return top;
  }

  clear(): void {
    this.#heap.length = 0;
  }

  #at(index: number): QueuedMessage {
    return this.#heap[index] as QueuedMessage;
  }

  #swap(a: number, b: number): void {
    const left = this.#at(a);
    this.#heap[a] = this.#at(b);
    this.#heap[b] = left;
  }

  #siftUp(from: number): void {
    let index = from;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (!precedes(this.#at(index), this.#at(parent))) break;
      this.#swap(index, parent);
      index = parent;
    }
  }

  #siftDown(from: number): void {
    const size = this.#heap.length;
    let index = from;
    for (;;) {
      const left = index * 2 + 1;
      const right = left + 1;
      let best = index;

      if (left < size && precedes(this.#at(left), this.#at(best))) best = left;
      if (right < size && precedes(this.#at(right), this.#at(best))) best = right;
      if (best === index) break;

      this.#swap(index, best);
      index = best;
    }
  }
}
