# Parquet Tool

Parquet Tool is a TypeScript library with a C++ native addon that enables
reading and writing of Apache Parquet files without relying on any existing
npm parquet package. It implements the necessary Thrift and Parquet format
logic in C++, exposed via Node.js using N-API.

Key features:

- Read and write Parquet files supporting BOOLEAN, INT32, INT64, FLOAT,
  DOUBLE, and BYTE_ARRAY (string) types.
- Merge multiple Parquet files with schema compatibility checks.
- Validate Parquet structure, row groups, and column consistency.
- Convert between CSV and Parquet.
- Convert between Apache Arrow IPC files and Parquet.
- Optional (nullable) columns and append (apply) mode for adding data to an
  existing file.
- File splitting and parallel processing utilities for handling large datasets.
- Debug mode for CLI and library diagnostics.
- Command-line interface for common operations (`info`, `read`, `write`,
  `append`, `split`, `merge`, `validate`, `csv-to-parquet`,
  `parquet-to-csv`, `arrow-to-parquet`, `parquet-to-arrow`).
- Example code and Docker Compose viewer for easy verification.
- Built-in testing with Jest, linting with ESLint, and release tooling with
  semantic-release and Commitizen.
