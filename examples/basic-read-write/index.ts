import * as path from 'path';
import { ParquetReader, ParquetWriter, Schema } from '../../src/lib';

const schema = Schema.create({
  id: 'INT32',
  name: 'STRING',
  score: { type: 'DOUBLE', optional: true },
  active: { type: 'BOOLEAN', optional: true },
});

const outputPath = path.join(__dirname, 'basic.parquet');

const writer = new ParquetWriter(outputPath, schema, { rowGroupSize: 2 });
writer.write([
  { id: 1, name: 'Alice', score: 98.5, active: true },
  { id: 2, name: 'Bob', active: false },
]);
writer.close();

const appender = ParquetWriter.openForAppend(outputPath, { rowGroupSize: 2 });
appender.write([
  { id: 3, name: 'Charlie', score: 75.0, active: true },
  { id: 4, name: 'Diana', score: 88.25 },
]);
appender.close();

const reader = ParquetReader.open(outputPath);

try {
  console.log('file:', outputPath);
  console.log('metadata:', reader.getMetadata());
  console.log('rows:', reader.readRows());
} finally {
  reader.close();
}