import * as path from 'path';
import {
  mergeParquetFiles,
  ParquetReader,
  ParquetRow,
  ParquetWriter,
  Schema,
  validateParquetFile,
} from '../../src/lib';

const schema = Schema.create({
  id: 'INT32',
  department: 'STRING',
  salary: 'DOUBLE',
});

function writeFile(filePath: string, rows: ParquetRow[]): void {
  const writer = new ParquetWriter(filePath, schema);
  writer.write(rows);
  writer.close();
}

const part1Path = path.join(__dirname, 'part-1.parquet');
const part2Path = path.join(__dirname, 'part-2.parquet');
const mergedPath = path.join(__dirname, 'merged.parquet');

writeFile(part1Path, [
  { id: 1, department: 'engineering', salary: 120000 },
  { id: 2, department: 'design', salary: 97000 },
]);

writeFile(part2Path, [
  { id: 3, department: 'sales', salary: 105000 },
  { id: 4, department: 'support', salary: 83000 },
]);

mergeParquetFiles([part1Path, part2Path], mergedPath);

const report = validateParquetFile(mergedPath);

console.log('merged:', mergedPath);
console.log('valid:', report.valid);
console.log('issues:', report.issues);
console.log('metadata:', ParquetReader.readMetadata(mergedPath));