import net from "node:net";

import type { Logger } from "@taskrail/core";

import { BrokerError, ConnectionError, ProtocolError } from "../support/errors.js";
import { decodeBody, encodeFrame } from "../wire/codec.js";
import { FrameReader } from "../wire/frame-reader.js";
import type { WireRequest, WireResponse } from "../wire/types.js";

export interface ConnectionOptions {
  readonly host: string;
  readonly port: number;
  readonly connectTimeoutMs: number;
  readonly requestTimeoutMs: number;
  readonly label: string;
  readonly logger: Logger;
}

interface Pending {
  readonly correlationId: number;
  readonly resolve: (response: WireResponse) => void;
  readonly reject: (error: unknown) => void;
  readonly timer: NodeJS.Timeout;
}

/**
 * 소켓 하나. driftmq는 파이프라이닝이 없으므로 in-flight 요청은 항상 한 개이며,
 * 요청은 이 클래스가 직렬화한다. 동시성이 필요하면 커넥션을 늘린다.
 *
 * 핸드셰이크가 없어서 연결 직후 바로 요청 프레임을 쓴다. 연결이 끊기면 브로커가 그 연결의
 * 미ACK 메시지를 즉시 재전달 대상으로 되돌리므로, 재연결은 곧 재전달을 뜻한다.
 */
export class DriftConnection {
  readonly #options: ConnectionOptions;
  readonly #reader = new FrameReader();
  #socket: net.Socket | undefined;
  #connecting: Promise<net.Socket> | undefined;
  #pending: Pending | undefined;
  #tail: Promise<unknown> = Promise.resolve();
  #correlationId = 0;
  #closed = false;

  constructor(options: ConnectionOptions) {
    this.#options = options;
  }

  get connected(): boolean {
    return this.#socket !== undefined && !this.#socket.destroyed;
  }

  /**
   * 실패한 요청을 자동으로 재전송하지 않는다 — PUBLISH는 브로커에 이미 도달했을 수 있어
   * 재전송이 조용한 중복을 만든다. 재시도 판단은 호출자의 몫이다.
   */
  async send(request: WireRequest): Promise<WireResponse> {
    const run = this.#tail.then(
      () => this.#exchange(request),
      () => this.#exchange(request),
    );
    this.#tail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#abort(new ConnectionError(`${this.#options.label} 커넥션이 close되었다`));
  }

  async #exchange(request: WireRequest): Promise<WireResponse> {
    if (this.#closed) {
      throw new ConnectionError(`${this.#options.label} 커넥션이 이미 close되었다`);
    }

    const socket = await this.#ensureSocket();
    const correlationId = this.#nextCorrelationId();
    const frame = encodeFrame(request, correlationId);

    return new Promise<WireResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#abort(
          new ConnectionError(
            `${request.kind} 응답이 ${this.#options.requestTimeoutMs}ms 안에 오지 않았다`,
          ),
        );
      }, this.#options.requestTimeoutMs);

      this.#pending = { correlationId, resolve, reject, timer };
      socket.write(frame, (error) => {
        if (error !== undefined && error !== null) {
          this.#abort(new ConnectionError("요청 프레임 쓰기 실패", { cause: error }));
        }
      });
    });
  }

  #nextCorrelationId(): number {
    this.#correlationId = (this.#correlationId % 0x7fff_ffff) + 1;
    return this.#correlationId;
  }

  async #ensureSocket(): Promise<net.Socket> {
    if (this.#socket !== undefined && !this.#socket.destroyed) return this.#socket;
    this.#connecting ??= this.#connect().finally(() => {
      this.#connecting = undefined;
    });
    return this.#connecting;
  }

  async #connect(): Promise<net.Socket> {
    const { host, port, connectTimeoutMs, label } = this.#options;

    return new Promise<net.Socket>((resolve, reject) => {
      const socket = net.connect({ host, port });
      socket.setNoDelay(true);
      socket.setTimeout(connectTimeoutMs);

      const onFailure = (error: unknown): void => {
        socket.destroy();
        reject(new ConnectionError(`${label} 연결 실패 (${host}:${port})`, { cause: error }));
      };

      socket.once("error", onFailure);
      socket.once("timeout", () => onFailure(new Error(`connect timeout ${connectTimeoutMs}ms`)));
      socket.once("connect", () => {
        socket.setTimeout(0);
        socket.off("error", onFailure);
        this.#attach(socket);
        resolve(socket);
      });
    });
  }

  #attach(socket: net.Socket): void {
    this.#socket = socket;
    this.#reader.reset();

    socket.on("data", (chunk: Buffer) => this.#onData(chunk));
    socket.on("error", (error) => {
      this.#abort(new ConnectionError(`${this.#options.label} 소켓 오류`, { cause: error }));
    });
    socket.on("close", () => {
      if (this.#socket === socket) this.#socket = undefined;
      this.#abort(new ConnectionError(`${this.#options.label} 연결이 끊겼다`));
    });
  }

  #onData(chunk: Buffer): void {
    this.#reader.push(chunk);

    for (;;) {
      let response: WireResponse | undefined;
      try {
        const body = this.#reader.next();
        if (body === undefined) return;
        response = decodeBody(body);
      } catch (error) {
        this.#abort(
          error instanceof ProtocolError
            ? error
            : new ProtocolError("응답 프레임 디코딩 실패", { cause: error }),
        );
        return;
      }
      this.#settle(response);
    }
  }

  #settle(response: WireResponse): void {
    const pending = this.#pending;
    if (pending === undefined) {
      this.#abort(new ProtocolError("보낸 적 없는 요청의 응답이 도착했다"));
      return;
    }

    if (response.correlationId !== pending.correlationId) {
      this.#abort(
        new ProtocolError(
          `correlationId 불일치: 기대 ${pending.correlationId}, 수신 ${response.correlationId}`,
        ),
      );
      return;
    }

    this.#pending = undefined;
    clearTimeout(pending.timer);

    if (response.kind !== "error") {
      pending.resolve(response);
      return;
    }

    const error = new BrokerError(response.code, response.message);
    // MALFORMED_FRAME은 브로커가 ERROR 직후 연결을 끊는다. 소켓을 살려 두지 않는다.
    if (error.fatal) this.#destroySocket();
    pending.reject(error);
  }

  /** in-flight 요청을 실패시키고 소켓을 버린다. 다음 요청이 새로 연결한다. */
  #abort(error: unknown): void {
    const pending = this.#pending;
    this.#pending = undefined;
    this.#destroySocket();

    if (pending === undefined) return;
    clearTimeout(pending.timer);
    pending.reject(error);
  }

  #destroySocket(): void {
    const socket = this.#socket;
    this.#socket = undefined;
    this.#reader.reset();
    if (socket === undefined) return;

    this.#options.logger.debug("driftmq 소켓을 버린다", { label: this.#options.label });
    socket.removeAllListeners();
    socket.destroy();
  }
}
