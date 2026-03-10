import { ParquetType, SchemaColumn, ParquetSchema } from './types';

/** Fluent builder for Parquet schemas. */
export class Schema {
  private columns: SchemaColumn[] = [];

  /** Add a column to the schema. */
  addColumn(name: string, type: ParquetType | string, optional = false): this {
    const resolvedType = typeof type === 'string' ? Schema.parseType(type) : type;
    this.columns.push({ name, type: resolvedType, optional });
    return this;
  }

  /** Parse a human‑friendly type name to the enum value. */
  static parseType(type: string): ParquetType {
    switch (type.toUpperCase()) {
      case 'BOOLEAN':
      case 'BOOL':
        return ParquetType.BOOLEAN;
      case 'INT32':
      case 'INT':
      case 'INTEGER':
        return ParquetType.INT32;
      case 'INT64':
      case 'LONG':
      case 'BIGINT':
        return ParquetType.INT64;
      case 'FLOAT':
        return ParquetType.FLOAT;
      case 'DOUBLE':
        return ParquetType.DOUBLE;
      case 'BYTE_ARRAY':
      case 'STRING':
      case 'UTF8':
        return ParquetType.BYTE_ARRAY;
      default:
        throw new Error(`Unknown Parquet type: ${type}`);
    }
  }

  /** Validate and return the built schema. */
  build(): ParquetSchema {
    if (this.columns.length === 0)
      throw new Error('Schema must have at least one column');
    return { columns: [...this.columns] };
  }

  /**
   * Shorthand to create a schema from a plain definition object.
   *
   * ```ts
   * Schema.create({
   *   id: 'INT32',
   *   name: { type: 'STRING', optional: true },
   * });
   * ```
   */
  static create(
    definition: Record<string, string | { type: string; optional?: boolean }>,
  ): ParquetSchema {
    const builder = new Schema();
    for (const [name, def] of Object.entries(definition)) {
      if (typeof def === 'string') {
        builder.addColumn(name, def);
      } else {
        builder.addColumn(name, def.type, def.optional);
      }
    }
    return builder.build();
  }
}
