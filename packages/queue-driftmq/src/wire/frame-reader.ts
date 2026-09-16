import { ProtocolError } from "../support/errors.js";
import { LENGTH_PREFIX_BYTES, MAX_FRAME_BYTES } from "./protocol.js";

/**
 * TCP는 프레임 경계를 보장하지 않는다. 완전한 프레임이 없으면 아무것도 소비하지 않고
 * 더 올 때까지 기다린다 — 이게 브로커 쪽 디코더와 맞춘 계약이다.
 */
export class FrameReader {
  #buf: Buffer = Buffer.alloc(0);
  #offset = 0;

  get buffered(): number {
    return this.#buf.length - this.#offset;
  }

  push(chunk: Buffer): void {
    this.#buf = this.buffered === 0 ? chunk : Buffer.concat([this.#rest(), chunk]);
    this.#offset = 0;
  }

  /** 다음 프레임의 body. 아직 완전하지 않으면 undefined. */
  next(): Buffer | undefined {
    if (this.buffered < LENGTH_PREFIX_BYTES) return undefined;

    const bodyLength = this.#buf.readUInt32BE(this.#offset);
    if (bodyLength > MAX_FRAME_BYTES) {
      throw new ProtocolError(
        `프레임이 너무 크다: ${bodyLength} > ${MAX_FRAME_BYTES}`,
      );
    }
    if (this.buffered < LENGTH_PREFIX_BYTES + bodyLength) return undefined;

    const start = this.#offset + LENGTH_PREFIX_BYTES;
    const body = Buffer.from(this.#buf.subarray(start, start + bodyLength));
    this.#offset = start + bodyLength;

    if (this.buffered === 0) this.reset();
    return body;
  }

  reset(): void {
    this.#buf = Buffer.alloc(0);
    this.#offset = 0;
  }

  #rest(): Buffer {
    return this.#buf.subarray(this.#offset);
  }
}
