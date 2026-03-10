import * as path from 'path';

interface NativeAddon {
  createWriter(filePath: string, schema: any[]): number;
  writeRowGroup(handle: number, columns: any[]): void;
  closeWriter(handle: number): void;

  openReader(filePath: string): { handle: number; metadata: any };
  readRowGroup(handle: number, rowGroupIndex: number): any;
  closeReader(handle: number): void;

  getMetadata(filePath: string): any;

  openAppender(filePath: string): { handle: number; metadata: any };
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
