import * as fs from 'fs';
import * as path from 'path';
import {
  arrowToParquet,
  csvToParquet,
  parquetToArrow,
  parquetToCsv,
  ParquetReader,
} from '../../src/lib';

const csvPath = path.join(__dirname, 'input.csv');
const parquetPath = path.join(__dirname, 'from-csv.parquet');
const roundtripCsvPath = path.join(__dirname, 'roundtrip.csv');
const arrowPath = path.join(__dirname, 'table.arrow');
const restoredParquetPath = path.join(__dirname, 'from-arrow.parquet');

fs.writeFileSync(
  csvPath,
  ['id,name,score', '1,Alice,98.5', '2,Bob,82', '3,Charlie,91.25'].join('\n'),
  'utf8',
);

const schema = csvToParquet(csvPath, parquetPath);
parquetToCsv(parquetPath, roundtripCsvPath);
parquetToArrow(parquetPath, arrowPath);
arrowToParquet(arrowPath, restoredParquetPath);

const reader = ParquetReader.open(restoredParquetPath);

try {
  console.log('schema:', schema);
  console.log('restored rows:', reader.readRows());
  console.log('generated files:', {
    csvPath,
    parquetPath,
    roundtripCsvPath,
    arrowPath,
    restoredParquetPath,
  });
} finally {
  reader.close();
}