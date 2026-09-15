export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

export const noopLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};

export function withContext(logger: Logger, context: Record<string, unknown>): Logger {
  const merge = (meta?: Record<string, unknown>): Record<string, unknown> =>
    meta === undefined ? context : { ...context, ...meta };

  return {
    debug: (message, meta) => logger.debug(message, merge(meta)),
    info: (message, meta) => logger.info(message, merge(meta)),
    warn: (message, meta) => logger.warn(message, merge(meta)),
    error: (message, meta) => logger.error(message, merge(meta)),
  };
}
