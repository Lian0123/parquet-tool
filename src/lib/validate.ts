import { debugLog } from './debug';
import { ParquetReader } from './reader';
import { ValidationIssue, ValidationResult } from './types';

export function validateParquetFile(filePath: string): ValidationResult {
  const issues: ValidationIssue[] = [];
  let reader: ParquetReader | null = null;

  try {
    reader = ParquetReader.open(filePath);
    const metadata = reader.getMetadata();
    debugLog('validate: metadata loaded', {
      filePath,
      numRows: metadata.numRows,
      numRowGroups: metadata.numRowGroups,
    });

    if (metadata.schema.length === 0) {
      issues.push({
        level: 'error',
        code: 'EMPTY_SCHEMA',
        message: 'Parquet schema is empty.',
      });
    }

    let totalRows = 0;
    metadata.rowGroups.forEach((rowGroup, index) => {
      totalRows += rowGroup.numRows;

      if (rowGroup.columns.length !== metadata.schema.length) {
        issues.push({
          level: 'error',
          code: 'COLUMN_COUNT_MISMATCH',
          message: `Row group ${index} column count ${rowGroup.columns.length} does not match schema column count ${metadata.schema.length}.`,
        });
      }

      if (rowGroup.numRows < 0) {
        issues.push({
          level: 'error',
          code: 'NEGATIVE_ROW_COUNT',
          message: `Row group ${index} has a negative row count.`,
        });
      }

      const data = reader?.readRowGroup(index);
      if (!data) {
        return;
      }

      for (const column of metadata.schema) {
        const values = data.columns[column.name];
        if (!values) {
          issues.push({
            level: 'error',
            code: 'MISSING_COLUMN_DATA',
            message: `Row group ${index} is missing data for column ${column.name}.`,
          });
          continue;
        }

        if (values.length !== rowGroup.numRows) {
          issues.push({
            level: 'error',
            code: 'ROW_GROUP_LENGTH_MISMATCH',
            message: `Row group ${index} column ${column.name} has ${values.length} values, expected ${rowGroup.numRows}.`,
          });
        }
      }
    });

    if (totalRows !== metadata.numRows) {
      issues.push({
        level: 'error',
        code: 'TOTAL_ROW_MISMATCH',
        message: `Metadata row count ${metadata.numRows} does not match summed row group rows ${totalRows}.`,
      });
    }

    return {
      valid: issues.every((issue) => issue.level !== 'error'),
      filePath,
      metadata,
      issues,
    };
  } catch (error) {
    debugLog('validate: failed', error);
    return {
      valid: false,
      filePath,
      metadata: null,
      issues: [
        {
          level: 'error',
          code: 'READ_FAILURE',
          message: error instanceof Error ? error.message : String(error),
        },
      ],
    };
  } finally {
    reader?.close();
  }
}