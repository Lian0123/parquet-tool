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

export function configureDebugMode(options: DebugOptions): void {
  if (typeof options.enabled === 'boolean') {
    debugState.enabled = options.enabled;
  }

  if (options.logger) {
    debugState.logger = options.logger;
  }
}

export function setDebugMode(enabled: boolean): void {
  debugState.enabled = enabled;
}

export function isDebugEnabled(): boolean {
  return debugState.enabled;
}

export function debugLog(message: string, payload?: unknown): void {
  if (!debugState.enabled) {
    return;
  }

  debugState.logger(message, payload);
}