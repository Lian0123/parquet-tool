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

// node-gyp-build resolves pre-built binaries in the following order:
//   1. prebuilds/{platform}-{arch}/*.node  (bundled in the npm package)
//   2. build/Release/*.node                (cmake-js output when building from source)
//   3. build/Debug/*.node                  (cmake-js debug build)
//
// Path resolution:
//   - compiled output lives in dist/lib/  → ../../ == package root
//   - ts-jest runs from     src/lib/      → ../../ == package root
// eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
const nodeGypBuild = require('node-gyp-build') as (dir: string) => NativeAddon;

let addon: NativeAddon;
try {
  addon = nodeGypBuild(path.join(__dirname, '../..'));
} catch (err) {
  throw new Error(
    'Failed to load native parquet addon.\n' +
      'If installing from source, run: npm run build:native\n' +
      `Details: ${err instanceof Error ? err.message : String(err)}`,
  );
}

export const native = addon;
