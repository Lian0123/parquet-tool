import * as fs from 'fs';
import * as path from 'path';
import {
  arrowToParquet,
  csvToParquet,
  jsonToParquet,
  parquetToArrow,
  parquetToCsv,
  parquetToJson,
  parquetToXml,
  ParquetReader,
  xmlToParquet,
} from '../../src/lib';

const csvPath = path.join(__dirname, 'input.csv');
const parquetPath = path.join(__dirname, 'from-csv.parquet');
const roundtripCsvPath = path.join(__dirname, 'roundtrip.csv');
const arrowPath = path.join(__dirname, 'table.arrow');
const restoredParquetPath = path.join(__dirname, 'from-arrow.parquet');
const jsonPath = path.join(__dirname, 'table.json');
const jsonRoundtripPath = path.join(__dirname, 'from-json.parquet');
const xmlPath = path.join(__dirname, 'table.xml');
const xmlRoundtripPath = path.join(__dirname, 'from-xml.parquet');

fs.writeFileSync(
  csvPath,
  ['id,name,score', '1,Alice,98.5', '2,Bob,82', '3,Charlie,91.25'].join('\n'),
  'utf8',
);

const schema = csvToParquet(csvPath, parquetPath);
parquetToCsv(parquetPath, roundtripCsvPath);
parquetToArrow(parquetPath, arrowPath);
arrowToParquet(arrowPath, restoredParquetPath);
parquetToJson(parquetPath, jsonPath);
jsonToParquet(jsonPath, jsonRoundtripPath);
parquetToXml(parquetPath, xmlPath);
xmlToParquet(xmlPath, xmlRoundtripPath);

const reader = ParquetReader.open(xmlRoundtripPath);

try {
  console.log('schema:', schema);
  console.log('restored rows:', reader.readRows());
  console.log('generated files:', {
    csvPath,
    parquetPath,
    roundtripCsvPath,
    arrowPath,
    restoredParquetPath,
    jsonPath,
    jsonRoundtripPath,
    xmlPath,
    xmlRoundtripPath,
  });
} finally {
  reader.close();
}