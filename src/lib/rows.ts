import { ParquetColumns, ParquetRow, RowGroupData } from './types';

export function rowGroupToRows(rowGroup: RowGroupData): ParquetRow[] {
  const names = Object.keys(rowGroup.columns);
  const rows: ParquetRow[] = [];

  for (let index = 0; index < rowGroup.numRows; index++) {
    const row: ParquetRow = {};
    for (const name of names) {
      row[name] = rowGroup.columns[name][index] ?? null;
    }
    rows.push(row);
  }

  return rows;
}

export function rowsToColumns(rows: ParquetRow[]): ParquetColumns {
  const columns: ParquetColumns = {};

  for (const row of rows) {
    for (const [name, value] of Object.entries(row)) {
      columns[name] ??= [];
      columns[name].push(value ?? null);
    }
  }

  return columns;
}