import * as path from 'path';
import {
  FileMetadata,
  NativeReaderHandle,
  NativeSchemaColumn,
  NativeWriteColumn,
  RowGroupData,
} from './types';

interface NativeAddon {
  createWriter(filePath: string, schema: NativeSchemaColumn[]): number;
  writeRowGroup(handle: number, columns: NativeWriteColumn[]): void;
  closeWriter(handle: number): void;

  openReader(filePath: string): NativeReaderHandle;
  readRowGroup(handle: number, rowGroupIndex: number): RowGroupData;
  closeReader(handle: number): void;

  getMetadata(filePath: string): FileMetadata;

  openAppender(filePath: string): NativeReaderHandle;
}

let addon: NativeAddon;

const tryPaths = [
  path.resolve(__dirname, '../../build/Release/parquet_addon.node'),
  path.resolve(__dirname, '../../build/Debug/parquet_addon.node'),
];

for (const p of tryPaths) {
  try {
    addon = require(p);
    break;
  } catch {
    // try next
  }
}

if (!addon!) {
  throw new Error(
    'Failed to load native parquet addon. Run "npm run build:native" first.',
  );
}

export const native = addon!;
