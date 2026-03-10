import * as path from 'path';
import { ParquetReader } from './reader';
import { ParquetWriter } from './writer';

export interface SplitOptions {
  /** Max rows per output file. */
  maxRowsPerFile?: number;
  /** Output directory (defaults to same directory as input). */
  outputDir?: string;
  /** File name prefix (defaults to input file base name). */
  prefix?: string;
}

/**
 * Split a single Parquet file into multiple smaller files.
 *
 * Returns the list of output file paths.
 */
export function splitParquetFile(
  inputPath: string,
  options: SplitOptions = {},
): string[] {
  const {
    maxRowsPerFile = 100_000,
    outputDir = path.dirname(inputPath),
    prefix = path.basename(inputPath, '.parquet'),
  } = options;

  const reader = ParquetReader.open(inputPath);
  const schema = reader.getSchema();
  const metadata = reader.getMetadata();
  const outputFiles: string[] = [];

  let currentWriter: ParquetWriter | null = null;
  let currentRowCount = 0;
  let fileIndex = 0;

  const startNewFile = (): void => {
    if (currentWriter) currentWriter.close();
    const fileName = `${prefix}_part${String(fileIndex).padStart(4, '0')}.parquet`;
    const filePath = path.join(outputDir, fileName);
    currentWriter = new ParquetWriter(filePath, schema, {
      rowGroupSize: maxRowsPerFile,
    });
    outputFiles.push(filePath);
    currentRowCount = 0;
    fileIndex++;
  };

  startNewFile();

  for (let i = 0; i < metadata.numRowGroups; i++) {
    const rg = reader.readRowGroup(i);
    const colNames = Object.keys(rg.columns);

    for (let r = 0; r < rg.numRows; r++) {
      if (currentRowCount >= maxRowsPerFile) {
        startNewFile();
      }
      const row: Record<string, any> = {};
      for (const name of colNames) {
        row[name] = rg.columns[name][r];
      }
      currentWriter!.write(row);
      currentRowCount++;
    }
  }

  (currentWriter as ParquetWriter | null)?.close();
  reader.close();
  return outputFiles;
}
