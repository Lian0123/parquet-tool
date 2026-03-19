import { debugLog } from './debug';
import { ParquetReader } from './reader';
import { rowGroupToRows } from './rows';
import { MergeOptions, ParquetSchema } from './types';
import { validateParquetFile } from './validate';
import { ParquetWriter } from './writer';

function schemasEqual(left: ParquetSchema, right: ParquetSchema): boolean {
  return JSON.stringify(left.columns) === JSON.stringify(right.columns);
}

/**
 * Merge multiple Parquet files into a single output file.
 *
 * By default, this validates inputs and enforces schema compatibility.
 */
export function mergeParquetFiles(
  inputPaths: string[],
  outputPath: string,
  options: MergeOptions = {},
): void {
  if (inputPaths.length === 0) {
    throw new Error('At least one input Parquet file is required.');
  }

  const validateSchema = options.validateSchema ?? true;

  if (validateSchema) {
    for (const inputPath of inputPaths) {
      const validation = validateParquetFile(inputPath);
      if (!validation.valid) {
        throw new Error(
          `Input file failed validation: ${inputPath} (${validation.issues[0]?.message ?? 'unknown error'})`,
        );
      }
    }
  }

  const firstReader = ParquetReader.open(inputPaths[0]);
  const schema = firstReader.getSchema();
  firstReader.close();

  const writer = new ParquetWriter(outputPath, schema, options);

  try {
    for (const inputPath of inputPaths) {
      const reader = ParquetReader.open(inputPath);
      const currentSchema = reader.getSchema();

      if (validateSchema && !schemasEqual(schema, currentSchema)) {
        reader.close();
        throw new Error(`Schema mismatch while merging file: ${inputPath}`);
      }

      debugLog('merge: reading source file', { inputPath });
      for (let index = 0; index < reader.getMetadata().numRowGroups; index++) {
        const rows = rowGroupToRows(reader.readRowGroup(index));
        writer.write(rows);
      }
      reader.close();
    }
  } finally {
    writer.close();
  }
}