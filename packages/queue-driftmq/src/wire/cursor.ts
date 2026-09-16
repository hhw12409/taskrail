import { ProtocolError } from "../support/errors.js";
import { U16_MAX } from "./protocol.js";
import type { WireHeader } from "./types.js";

/**
 * 모든 정수는 big-endian, 문자열은 UTF-8이다. string은 u16 길이 prefix,
 * bytes(payload/header value)는 i32 길이 prefix로 폭이 다르다.
 */
export class ByteWriter {
  readonly #parts: Buffer[] = [];
  #length = 0;

  get length(): number {
    return this.#length;
  }

  u8(value: number): this {
    const buf = Buffer.allocUnsafe(1);
    buf.writeUInt8(value & 0xff, 0);
    return this.#push(buf);
  }

  u16(value: number): this {
    const buf = Buffer.allocUnsafe(2);
    buf.writeUInt16BE(value & 0xffff, 0);
    return this.#push(buf);
  }

  i32(value: number): this {
    const buf = Buffer.allocUnsafe(4);
    buf.writeInt32BE(value | 0, 0);
    return this.#push(buf);
  }

  i64(value: bigint): this {
    const buf = Buffer.allocUnsafe(8);
    buf.writeBigInt64BE(value, 0);
    return this.#push(buf);
  }

  string(value: string): this {
    const bytes = Buffer.from(value, "utf8");
    if (bytes.length > U16_MAX) {
      throw new ProtocolError(`string이 ${U16_MAX}바이트를 초과한다: ${bytes.length}`);
    }
    return this.u16(bytes.length).#push(bytes);
  }

  bytes(value: Buffer): this {
    return this.i32(value.length).#push(value);
  }

  headers(headers: readonly WireHeader[]): this {
    if (headers.length > U16_MAX) {
      throw new ProtocolError(`헤더 개수가 ${U16_MAX}를 초과한다: ${headers.length}`);
    }
    this.u16(headers.length);
    for (const header of headers) {
      this.string(header.key).bytes(header.value);
    }
    return this;
  }

  toBuffer(): Buffer {
    return Buffer.concat(this.#parts, this.#length);
  }

  #push(buf: Buffer): this {
    this.#parts.push(buf);
    this.#length += buf.length;
    return this;
  }
}

export class ByteReader {
  readonly #buf: Buffer;
  #offset = 0;

  constructor(buf: Buffer) {
    this.#buf = buf;
  }

  get remaining(): number {
    return this.#buf.length - this.#offset;
  }

  u8(): number {
    return this.#buf.readUInt8(this.#take(1));
  }

  u16(): number {
    return this.#buf.readUInt16BE(this.#take(2));
  }

  i32(): number {
    return this.#buf.readInt32BE(this.#take(4));
  }

  i64(): bigint {
    return this.#buf.readBigInt64BE(this.#take(8));
  }

  string(): string {
    const length = this.u16();
    const start = this.#take(length);
    return this.#buf.toString("utf8", start, start + length);
  }

  bytes(): Buffer {
    const length = this.i32();
    if (length < 0) {
      throw new ProtocolError(`bytes 길이가 음수다: ${length}`);
    }
    const start = this.#take(length);
    return Buffer.from(this.#buf.subarray(start, start + length));
  }

  headers(): WireHeader[] {
    const count = this.u16();
    this.#requireCount(count);
    const headers: WireHeader[] = [];
    for (let i = 0; i < count; i += 1) {
      headers.push({ key: this.string(), value: this.bytes() });
    }
    return headers;
  }

  /** 항목 하나가 최소 1바이트는 차지한다 — 남은 바이트보다 큰 count는 손상이다. */
  #requireCount(count: number): void {
    if (count < 0 || count > this.remaining + 1) {
      throw new ProtocolError(`count ${count}가 남은 ${this.remaining}바이트를 넘는다`);
    }
  }

  #take(length: number): number {
    if (length < 0 || this.remaining < length) {
      throw new ProtocolError(`프레임이 잘렸다: ${length}바이트 필요, ${this.remaining} 남음`);
    }
    const start = this.#offset;
    this.#offset += length;
    return start;
  }
}
