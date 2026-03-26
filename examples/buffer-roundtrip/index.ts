import * as path from 'path';
import {
  bufferToPaquet,
  paquetToBuffer,
  ParquetReader,
  ParquetWriter,
  Schema,
  validateParquetFile,
} from '../../src/lib';

const schema = Schema.create({
  id: 'INT32',
  name: 'STRING',
  age: 'INT32',
});

const sourcePath = path.join(__dirname, 'source.parquet');
const copyPath = path.join(__dirname, 'copy.parquet');

const writer = new ParquetWriter(sourcePath, schema);
writer.write([
  { id: 1, name: 'Alice', age: 30 },
  { id: 2, name: 'Bob', age: 27 },
]);
writer.close();

const buffer = paquetToBuffer(sourcePath, { validate: true });
bufferToPaquet(buffer, copyPath, { overwrite: true, validate: true });

const report = validateParquetFile(copyPath);
const reader = ParquetReader.open(copyPath);

try {
  console.log('bytes:', buffer.length);
  console.log('valid:', report.valid);
  console.log('rows:', reader.readRows());
} finally {
  reader.close();
}