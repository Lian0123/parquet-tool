import * as fs from 'fs';
import { validateParquetFile } from './validate';

export interface PaquetToBufferOptions {
  /** When true, validate the Parquet structure before returning the buffer. */
  validate?: boolean;
}

export interface BufferToPaquetOptions {
  /** When false (default), throws if the output file already exists. */
  overwrite?: boolean;
  /** When true, validate the written Parquet file after writing. */
  validate?: boolean;
}

/**
 * Read a Parquet file and return its raw bytes as a Node.js Buffer.
 *
 * Note: this is a byte-level conversion (no decoding). Use `ParquetReader`
 * if you want to read rows/columns.
 */
export function paquetToBuffer(
  filePath: string,
  options: PaquetToBufferOptions = {},
): Buffer {
  if (!filePath) {
    throw new TypeError('filePath is required');
  }

  if (options.validate) {
    const report = validateParquetFile(filePath);
    if (!report.valid) {
      const msg = report.issues[0]?.message ?? 'unknown validation error';
      throw new Error(`Invalid Parquet file: ${filePath} (${msg})`);
    }
  }

  return fs.readFileSync(filePath);
}

/**
 * Write a raw Parquet buffer to a file.
 *
 * Note: this is a byte-level conversion (no encoding). Use `ParquetWriter`
 * if you want to build a Parquet file from rows.
 */
export function bufferToPaquet(
  buffer: Buffer,
  filePath: string,
  options: BufferToPaquetOptions = {},
): void {
  if (!Buffer.isBuffer(buffer)) {
    throw new TypeError('buffer must be a Buffer');
  }
  if (!filePath) {
    throw new TypeError('filePath is required');
  }

  const overwrite = options.overwrite ?? false;
  const flag = overwrite ? 'w' : 'wx';
  fs.writeFileSync(filePath, buffer, { flag });

  if (options.validate) {
    const report = validateParquetFile(filePath);
    if (!report.valid) {
      const msg = report.issues[0]?.message ?? 'unknown validation error';
      throw new Error(`Written Parquet file failed validation: ${filePath} (${msg})`);
    }
  }
}
