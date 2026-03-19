import { DebugOptions } from './types';

type DebugState = {
  enabled: boolean;
  logger: (message: string, payload?: unknown) => void;
};

const debugState: DebugState = {
  enabled: process.env.PARQUET_TOOL_DEBUG === '1',
  logger: (message, payload) => {
    if (payload === undefined) {
      console.error(`[parquet-tool:debug] ${message}`);
      return;
    }

    console.error(`[parquet-tool:debug] ${message}`, payload);
  },
};

/**
 * Configure debug mode globally for this library.
 *
 * - `enabled`: turns debug logging on/off
 * - `logger`: custom logger implementation
 */
export function configureDebugMode(options: DebugOptions): void {
  if (typeof options.enabled === 'boolean') {
    debugState.enabled = options.enabled;
  }

  if (options.logger) {
    debugState.logger = options.logger;
  }
}

/** Enable/disable debug logging. */
export function setDebugMode(enabled: boolean): void {
  debugState.enabled = enabled;
}

/** Returns whether debug logging is currently enabled. */
export function isDebugEnabled(): boolean {
  return debugState.enabled;
}

/**
 * Write a debug log message when debug mode is enabled.
 *
 * Use `configureDebugMode()` to control output and destination.
 */
export function debugLog(message: string, payload?: unknown): void {
  if (!debugState.enabled) {
    return;
  }

  debugState.logger(message, payload);
}