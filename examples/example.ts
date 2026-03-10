import { Schema, ParquetWriter, ParquetReader } from '../src/lib';

(async function main() {
  // define schema
  const schema = Schema.create({
    id: 'INT32',
    name: 'STRING',
    score: { type: 'DOUBLE', optional: true },
  });

  const file = 'examples/sample.parquet';

  // write some rows
  const writer = new ParquetWriter(file, schema);
  writer.write([
    { id: 1, name: 'Alice', score: 98.5 },
    { id: 2, name: 'Bob' }, // score omitted → null
    { id: 3, name: 'Charlie', score: 75.0 },
  ]);
  writer.close();
  console.log('Wrote', file);

  // read back
  const reader = ParquetReader.open(file);
  const data = reader.readAll();
  console.log('metadata:', reader.getMetadata());
  console.log('data:', data);
  reader.close();
})();