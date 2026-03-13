#pragma once
/**
 * Parquet file format implementation.
 * Supports: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY types.
 * Encoding: PLAIN only.  Compression: UNCOMPRESSED only.
 * Schema: flat (no nesting), REQUIRED and OPTIONAL fields.
 */

#include "thrift.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace parquet {

// ============================================================
// Parquet enums (match the Thrift spec values)
// ============================================================
enum Type : int32_t {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
};

enum Encoding : int32_t {
    PLAIN = 0,
    RLE = 3,
    BIT_PACKED = 4,
};

enum CompressionCodec : int32_t {
    UNCOMPRESSED = 0,
};

enum PageType : int32_t {
    DATA_PAGE = 0,
};

enum FieldRepetitionType : int32_t {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
};

enum ConvertedType : int32_t {
    CT_UTF8 = 0,
};

// ============================================================
// Metadata structs
// ============================================================
struct SchemaElement {
    std::string name;
    int32_t num_children = -1;   // -1 means leaf node
    int32_t type = -1;           // ParquetType, -1 if group
    int32_t converted_type = -1; // -1 if not set
    int32_t repetition_type = -1;
};

struct ColumnMetaData {
    int32_t type = 0;
    std::vector<int32_t> encodings;
    std::vector<std::string> path_in_schema;
    int32_t codec = UNCOMPRESSED;
    int64_t num_values = 0;
    int64_t total_uncompressed_size = 0;
    int64_t total_compressed_size = 0;
    int64_t data_page_offset = 0;
};

struct ColumnChunk {
    int64_t file_offset = 0;
    ColumnMetaData meta_data;
};

struct RowGroup {
    std::vector<ColumnChunk> columns;
    int64_t total_byte_size = 0;
    int64_t num_rows = 0;
};

struct FileMetaData {
    int32_t version = 2;
    std::vector<SchemaElement> schema;
    int64_t num_rows = 0;
    std::vector<RowGroup> row_groups;
    std::string created_by = "parquet-tool-cpp";
};

struct DataPageHeader {
    int32_t num_values = 0;
    int32_t encoding = PLAIN;
    int32_t definition_level_encoding = RLE;
    int32_t repetition_level_encoding = RLE;
};

struct PageHeader {
    int32_t type = DATA_PAGE;
    int32_t uncompressed_page_size = 0;
    int32_t compressed_page_size = 0;
    DataPageHeader data_page_header;
};

// Column schema for the API layer
struct ColSchema {
    std::string name;
    int32_t type;     // ParquetType
    bool optional;
};

// Column data for read/write
struct ColumnData {
    int32_t type;
    std::vector<int32_t> int32_vals;
    std::vector<int64_t> int64_vals;
    std::vector<float> float_vals;
    std::vector<double> double_vals;
    std::vector<uint8_t> bool_vals;
    std::vector<std::string> string_vals;
    std::vector<uint8_t> def_levels; // 0 = null, 1 = defined
    int32_t num_values = 0;
    int32_t num_nulls = 0;
    bool is_optional = false;
};

// ============================================================
// Thrift serialization helpers for Parquet structures
// ============================================================
namespace serial {

inline void writeSchemaElement(thrift::CompactWriter& w, const SchemaElement& e) {
    w.pushStruct();
    w.writeFieldString(1, e.name);
    if (e.num_children >= 0) w.writeFieldI32(2, e.num_children);
    if (e.type >= 0) w.writeFieldI32(3, e.type);
    if (e.converted_type >= 0) w.writeFieldI32(4, e.converted_type);
    if (e.repetition_type >= 0) w.writeFieldI32(6, e.repetition_type);
    w.popStruct();
}

inline void writeColumnMetaData(thrift::CompactWriter& w, const ColumnMetaData& m) {
    w.pushStruct();
    w.writeFieldI32(1, m.type);
    // encodings
    w.writeFieldListBegin(2, thrift::CT_I32, static_cast<int32_t>(m.encodings.size()));
    for (auto enc : m.encodings) w.writeZigzagI32(enc);
    // path_in_schema
    w.writeFieldListBegin(3, thrift::CT_BINARY, static_cast<int32_t>(m.path_in_schema.size()));
    for (auto& p : m.path_in_schema) w.writeBinary(p);
    w.writeFieldI32(4, m.codec);
    w.writeFieldI64(5, m.num_values);
    w.writeFieldI64(6, m.total_uncompressed_size);
    w.writeFieldI64(7, m.total_compressed_size);
    w.writeFieldI64(9, m.data_page_offset);
    w.popStruct();
}

inline void writeColumnChunk(thrift::CompactWriter& w, const ColumnChunk& cc) {
    w.pushStruct();
    w.writeFieldI64(2, cc.file_offset);
    w.writeFieldStructBegin(3);
    // inline ColumnMetaData writing
    auto& m = cc.meta_data;
    w.writeFieldI32(1, m.type);
    w.writeFieldListBegin(2, thrift::CT_I32, static_cast<int32_t>(m.encodings.size()));
    for (auto enc : m.encodings) w.writeZigzagI32(enc);
    w.writeFieldListBegin(3, thrift::CT_BINARY, static_cast<int32_t>(m.path_in_schema.size()));
    for (auto& p : m.path_in_schema) w.writeBinary(p);
    w.writeFieldI32(4, m.codec);
    w.writeFieldI64(5, m.num_values);
    w.writeFieldI64(6, m.total_uncompressed_size);
    w.writeFieldI64(7, m.total_compressed_size);
    w.writeFieldI64(9, m.data_page_offset);
    w.popStruct(); // end ColumnMetaData
    w.popStruct(); // end ColumnChunk
}

inline void writeRowGroup(thrift::CompactWriter& w, const RowGroup& rg) {
    w.pushStruct();
    w.writeFieldListBegin(1, thrift::CT_STRUCT, static_cast<int32_t>(rg.columns.size()));
    for (auto& cc : rg.columns) writeColumnChunk(w, cc);
    w.writeFieldI64(2, rg.total_byte_size);
    w.writeFieldI64(3, rg.num_rows);
    w.popStruct();
}

inline void writeFileMetaData(thrift::CompactWriter& w, const FileMetaData& meta) {
    w.pushStruct();
    w.writeFieldI32(1, meta.version);
    // schema
    w.writeFieldListBegin(2, thrift::CT_STRUCT, static_cast<int32_t>(meta.schema.size()));
    for (auto& se : meta.schema) writeSchemaElement(w, se);
    w.writeFieldI64(3, meta.num_rows);
    // row_groups
    w.writeFieldListBegin(4, thrift::CT_STRUCT, static_cast<int32_t>(meta.row_groups.size()));
    for (auto& rg : meta.row_groups) writeRowGroup(w, rg);
    w.writeFieldString(6, meta.created_by);
    w.popStruct();
}

inline void writePageHeader(thrift::CompactWriter& w, const PageHeader& ph) {
    w.pushStruct();
    w.writeFieldI32(1, ph.type);
    w.writeFieldI32(2, ph.uncompressed_page_size);
    w.writeFieldI32(3, ph.compressed_page_size);
    // data_page_header (field 5)
    w.writeFieldStructBegin(5);
    auto& dph = ph.data_page_header;
    w.writeFieldI32(1, dph.num_values);
    w.writeFieldI32(2, dph.encoding);
    w.writeFieldI32(3, dph.definition_level_encoding);
    w.writeFieldI32(4, dph.repetition_level_encoding);
    w.popStruct(); // end DataPageHeader
    w.popStruct(); // end PageHeader
}

// --- Reading ---

inline SchemaElement readSchemaElement(thrift::CompactReader& r) {
    SchemaElement se;
    r.pushStruct();
    for (;;) {
        auto fh = r.readFieldHeader();
        if (fh.type == thrift::CT_STOP) break;
        switch (fh.field_id) {
        case 1: se.name = r.readBinary(); break;
        case 2: se.num_children = r.readZigzagI32(); break;
        case 3: se.type = r.readZigzagI32(); break;
        case 4: se.converted_type = r.readZigzagI32(); break;
        case 6: se.repetition_type = r.readZigzagI32(); break;
        default: r.skip(fh.type); break;
        }
    }
    r.popStruct();
    return se;
}

inline ColumnMetaData readColumnMetaData(thrift::CompactReader& r) {
    ColumnMetaData cm;
    r.pushStruct();
    for (;;) {
        auto fh = r.readFieldHeader();
        if (fh.type == thrift::CT_STOP) break;
        switch (fh.field_id) {
        case 1: cm.type = r.readZigzagI32(); break;
        case 2: {
            auto lh = r.readListHeader();
            cm.encodings.resize(lh.count);
            for (int i = 0; i < lh.count; i++) cm.encodings[i] = r.readZigzagI32();
            break;
        }
        case 3: {
            auto lh = r.readListHeader();
            cm.path_in_schema.resize(lh.count);
            for (int i = 0; i < lh.count; i++) cm.path_in_schema[i] = r.readBinary();
            break;
        }
        case 4: cm.codec = r.readZigzagI32(); break;
        case 5: cm.num_values = r.readZigzagI64(); break;
        case 6: cm.total_uncompressed_size = r.readZigzagI64(); break;
        case 7: cm.total_compressed_size = r.readZigzagI64(); break;
        case 9: cm.data_page_offset = r.readZigzagI64(); break;
        default: r.skip(fh.type); break;
        }
    }
    r.popStruct();
    return cm;
}

inline ColumnChunk readColumnChunk(thrift::CompactReader& r) {
    ColumnChunk cc;
    r.pushStruct();
    for (;;) {
        auto fh = r.readFieldHeader();
        if (fh.type == thrift::CT_STOP) break;
        switch (fh.field_id) {
        case 2: cc.file_offset = r.readZigzagI64(); break;
        case 3: cc.meta_data = readColumnMetaData(r); break;
        default: r.skip(fh.type); break;
        }
    }
    r.popStruct();
    return cc;
}

inline RowGroup readRowGroup(thrift::CompactReader& r) {
    RowGroup rg;
    r.pushStruct();
    for (;;) {
        auto fh = r.readFieldHeader();
        if (fh.type == thrift::CT_STOP) break;
        switch (fh.field_id) {
        case 1: {
            auto lh = r.readListHeader();
            rg.columns.resize(lh.count);
            for (int i = 0; i < lh.count; i++) rg.columns[i] = readColumnChunk(r);
            break;
        }
        case 2: rg.total_byte_size = r.readZigzagI64(); break;
        case 3: rg.num_rows = r.readZigzagI64(); break;
        default: r.skip(fh.type); break;
        }
    }
    r.popStruct();
    return rg;
}

inline FileMetaData readFileMetaData(thrift::CompactReader& r) {
    FileMetaData meta;
    r.pushStruct();
    for (;;) {
        auto fh = r.readFieldHeader();
        if (fh.type == thrift::CT_STOP) break;
        switch (fh.field_id) {
        case 1: meta.version = r.readZigzagI32(); break;
        case 2: {
            auto lh = r.readListHeader();
            meta.schema.resize(lh.count);
            for (int i = 0; i < lh.count; i++) meta.schema[i] = readSchemaElement(r);
            break;
        }
        case 3: meta.num_rows = r.readZigzagI64(); break;
        case 4: {
            auto lh = r.readListHeader();
            meta.row_groups.resize(lh.count);
            for (int i = 0; i < lh.count; i++) meta.row_groups[i] = readRowGroup(r);
            break;
        }
        case 6: meta.created_by = r.readBinary(); break;
        default: r.skip(fh.type); break;
        }
    }
    r.popStruct();
    return meta;
}

inline PageHeader readPageHeader(thrift::CompactReader& r) {
    PageHeader ph;
    r.pushStruct();
    for (;;) {
        auto fh = r.readFieldHeader();
        if (fh.type == thrift::CT_STOP) break;
        switch (fh.field_id) {
        case 1: ph.type = r.readZigzagI32(); break;
        case 2: ph.uncompressed_page_size = r.readZigzagI32(); break;
        case 3: ph.compressed_page_size = r.readZigzagI32(); break;
        case 5: {
            // DataPageHeader
            r.pushStruct();
            for (;;) {
                auto fh2 = r.readFieldHeader();
                if (fh2.type == thrift::CT_STOP) break;
                switch (fh2.field_id) {
                case 1: ph.data_page_header.num_values = r.readZigzagI32(); break;
                case 2: ph.data_page_header.encoding = r.readZigzagI32(); break;
                case 3: ph.data_page_header.definition_level_encoding = r.readZigzagI32(); break;
                case 4: ph.data_page_header.repetition_level_encoding = r.readZigzagI32(); break;
                default: r.skip(fh2.type); break;
                }
            }
            r.popStruct();
            break;
        }
        default: r.skip(fh.type); break;
        }
    }
    r.popStruct();
    return ph;
}

} // namespace serial

// ============================================================
// Encoding helpers
// ============================================================
namespace encoding {

// Encode definition levels using RLE/Bit-Packing Hybrid (bit_width=1)
// Returns the full encoded data including 4-byte length prefix.
inline std::vector<uint8_t> encodeDefLevels(const std::vector<uint8_t>& levels, int32_t num_values) {
    // Use bit-packing: groups of 8 values
    int32_t num_groups = (num_values + 7) / 8;
    std::vector<uint8_t> rle_data;

    // Bit-packed header: varint((num_groups << 1) | 1)
    uint64_t header = (static_cast<uint64_t>(num_groups) << 1) | 1;
    while (header >= 0x80) {
        rle_data.push_back(static_cast<uint8_t>((header & 0x7F) | 0x80));
        header >>= 7;
    }
    rle_data.push_back(static_cast<uint8_t>(header));

    // Bit-packed values: bit_width=1, so each group = 1 byte
    for (int32_t g = 0; g < num_groups; g++) {
        uint8_t byte = 0;
        for (int b = 0; b < 8; b++) {
            int idx = g * 8 + b;
            if (idx < num_values && levels[idx]) {
                byte |= (1 << b);
            }
        }
        rle_data.push_back(byte);
    }

    // Prepend 4-byte length
    std::vector<uint8_t> result(4 + rle_data.size());
    uint32_t len = static_cast<uint32_t>(rle_data.size());
    std::memcpy(result.data(), &len, 4);
    std::memcpy(result.data() + 4, rle_data.data(), rle_data.size());
    return result;
}

// Decode definition levels from RLE/Bit-Packing Hybrid (bit_width=1)
inline std::vector<uint8_t> decodeDefLevels(const uint8_t* data, size_t data_size, int32_t num_values, size_t& bytes_read) {
    // First 4 bytes: length of encoded data
    if (data_size < 4) throw std::runtime_error("def levels: not enough data for length");
    uint32_t encoded_len;
    std::memcpy(&encoded_len, data, 4);
    bytes_read = 4 + encoded_len;

    std::vector<uint8_t> levels(num_values, 0);
    const uint8_t* p = data + 4;
    const uint8_t* end = p + encoded_len;
    int32_t values_read = 0;

    while (p < end && values_read < num_values) {
        // Read varint header
        uint64_t header = 0;
        int shift = 0;
        while (p < end) {
            uint8_t b = *p++;
            header |= static_cast<uint64_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }

        if (header & 1) {
            // Bit-packed run
            int32_t num_groups = static_cast<int32_t>(header >> 1);
            for (int32_t g = 0; g < num_groups && p < end; g++) {
                uint8_t byte = *p++;
                for (int b = 0; b < 8 && values_read < num_values; b++) {
                    levels[values_read++] = (byte >> b) & 1;
                }
            }
        } else {
            // RLE run
            int32_t run_len = static_cast<int32_t>(header >> 1);
            if (p >= end) break;
            uint8_t value = *p++;
            for (int32_t i = 0; i < run_len && values_read < num_values; i++) {
                levels[values_read++] = value & 1;
            }
        }
    }
    return levels;
}

// PLAIN encode values for a column
inline std::vector<uint8_t> encodePlainValues(int32_t type, const ColumnData& col) {
    std::vector<uint8_t> buf;
    int32_t non_null = col.num_values - col.num_nulls;

    switch (type) {
    case BOOLEAN: {
        int32_t num_bytes = (non_null + 7) / 8;
        buf.resize(num_bytes, 0);
        for (int32_t i = 0; i < non_null; i++) {
            if (col.bool_vals[i]) {
                buf[i / 8] |= (1 << (i % 8));
            }
        }
        break;
    }
    case INT32: {
        buf.resize(non_null * 4);
        std::memcpy(buf.data(), col.int32_vals.data(), non_null * 4);
        break;
    }
    case INT64: {
        buf.resize(non_null * 8);
        std::memcpy(buf.data(), col.int64_vals.data(), non_null * 8);
        break;
    }
    case FLOAT: {
        buf.resize(non_null * 4);
        std::memcpy(buf.data(), col.float_vals.data(), non_null * 4);
        break;
    }
    case DOUBLE: {
        buf.resize(non_null * 8);
        std::memcpy(buf.data(), col.double_vals.data(), non_null * 8);
        break;
    }
    case BYTE_ARRAY: {
        for (int32_t i = 0; i < non_null; i++) {
            uint32_t len = static_cast<uint32_t>(col.string_vals[i].size());
            size_t off = buf.size();
            buf.resize(off + 4 + len);
            std::memcpy(buf.data() + off, &len, 4);
            std::memcpy(buf.data() + off + 4, col.string_vals[i].data(), len);
        }
        break;
    }
    default:
        throw std::runtime_error("unsupported parquet type for encoding");
    }
    return buf;
}

// PLAIN decode values
inline void decodePlainValues(int32_t type, const uint8_t* data, size_t data_size,
                              int32_t count, ColumnData& col) {
    size_t off = 0;
    switch (type) {
    case BOOLEAN: {
        col.bool_vals.resize(count);
        for (int32_t i = 0; i < count; i++) {
            col.bool_vals[i] = (data[i / 8] >> (i % 8)) & 1;
        }
        break;
    }
    case INT32: {
        col.int32_vals.resize(count);
        std::memcpy(col.int32_vals.data(), data, count * 4);
        break;
    }
    case INT64: {
        col.int64_vals.resize(count);
        std::memcpy(col.int64_vals.data(), data, count * 8);
        break;
    }
    case FLOAT: {
        col.float_vals.resize(count);
        std::memcpy(col.float_vals.data(), data, count * 4);
        break;
    }
    case DOUBLE: {
        col.double_vals.resize(count);
        std::memcpy(col.double_vals.data(), data, count * 8);
        break;
    }
    case BYTE_ARRAY: {
        col.string_vals.resize(count);
        for (int32_t i = 0; i < count; i++) {
            if (off + 4 > data_size) throw std::runtime_error("truncated BYTE_ARRAY");
            uint32_t len;
            std::memcpy(&len, data + off, 4);
            off += 4;
            if (off + len > data_size) throw std::runtime_error("truncated BYTE_ARRAY data");
            col.string_vals[i].assign(reinterpret_cast<const char*>(data + off), len);
            off += len;
        }
        break;
    }
    default:
        throw std::runtime_error("unsupported parquet type for decoding");
    }
}

} // namespace encoding

// ============================================================
// Writer
// ============================================================
class Writer {
    FILE* file_ = nullptr;
    std::vector<ColSchema> schema_;
    std::vector<RowGroup> row_groups_;
    bool closed_ = false;

public:
    Writer() = default;
    ~Writer() { if (!closed_) close(); }

    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    void open(const std::string& path, const std::vector<ColSchema>& schema) {
        file_ = std::fopen(path.c_str(), "wb");
        if (!file_) throw std::runtime_error("cannot open file for writing: " + path);
        schema_ = schema;
        // Write magic
        std::fwrite("PAR1", 1, 4, file_);
    }

    void openForAppend(const std::string& path) {
        // 1) Read existing metadata
        FileMetaData existing;
        {
            FILE* rf = std::fopen(path.c_str(), "rb");
            if (!rf) throw std::runtime_error("cannot open file for append: " + path);
            // Read footer length + magic
            std::fseek(rf, -8, SEEK_END);
            uint8_t tail[8];
            std::fread(tail, 1, 8, rf);
            if (std::memcmp(tail + 4, "PAR1", 4) != 0)
                throw std::runtime_error("not a valid parquet file");
            uint32_t footer_len;
            std::memcpy(&footer_len, tail, 4);
            long footer_start = std::ftell(rf) - 8 - footer_len;
            std::fseek(rf, footer_start, SEEK_SET);
            std::vector<uint8_t> footer_buf(footer_len);
            std::fread(footer_buf.data(), 1, footer_len, rf);
            std::fclose(rf);
            thrift::CompactReader tr(footer_buf.data(), footer_buf.size());
            existing = serial::readFileMetaData(tr);
        }

        // 2) Rebuild schema from existing metadata
        schema_.clear();
        for (size_t i = 1; i < existing.schema.size(); i++) {
            auto& se = existing.schema[i];
            ColSchema cs;
            cs.name = se.name;
            cs.type = se.type;
            cs.optional = (se.repetition_type == OPTIONAL);
            schema_.push_back(cs);
        }

        // 3) Copy existing row groups
        row_groups_ = existing.row_groups;

        // 4) Open file for append, truncate after last data (before old footer)
        file_ = std::fopen(path.c_str(), "r+b");
        if (!file_) throw std::runtime_error("cannot open file for append: " + path);

        // Seek to footer start (where old footer was)
        std::fseek(file_, 0, SEEK_END);
        long file_size = std::ftell(file_);
        uint8_t tail[8];
        std::fseek(file_, file_size - 8, SEEK_SET);
        std::fread(tail, 1, 8, file_);
        uint32_t footer_len;
        std::memcpy(&footer_len, tail, 4);
        long truncate_pos = file_size - 8 - footer_len;

        // Truncate
#ifdef _WIN32
        _chsize(_fileno(file_), truncate_pos);
#else
        if (ftruncate(fileno(file_), truncate_pos) != 0) {
            throw std::runtime_error("ftruncate failed");
        }
#endif
        std::fseek(file_, truncate_pos, SEEK_SET);
    }

    void writeRowGroup(const std::vector<ColumnData>& columns) {
        if (!file_) throw std::runtime_error("writer not open");
        if (columns.size() != schema_.size())
            throw std::runtime_error("column count mismatch");

        RowGroup rg;
        rg.num_rows = columns[0].num_values;
        int64_t total_size = 0;

        for (size_t ci = 0; ci < columns.size(); ci++) {
            auto& col = columns[ci];
            auto& cs = schema_[ci];

            // Build page data
            std::vector<uint8_t> page_data;

            // Definition levels (only for OPTIONAL)
            if (cs.optional) {
                auto def_encoded = encoding::encodeDefLevels(col.def_levels, col.num_values);
                page_data.insert(page_data.end(), def_encoded.begin(), def_encoded.end());
            }

            // Encode values (only non-null values)
            auto value_data = encoding::encodePlainValues(cs.type, col);
            page_data.insert(page_data.end(), value_data.begin(), value_data.end());

            // Record data page offset
            int64_t data_page_offset = std::ftell(file_);

            // Write page header
            PageHeader ph;
            ph.type = DATA_PAGE;
            ph.uncompressed_page_size = static_cast<int32_t>(page_data.size());
            ph.compressed_page_size = static_cast<int32_t>(page_data.size());
            ph.data_page_header.num_values = col.num_values;
            ph.data_page_header.encoding = PLAIN;
            ph.data_page_header.definition_level_encoding = RLE;
            ph.data_page_header.repetition_level_encoding = RLE;

            thrift::CompactWriter tw;
            serial::writePageHeader(tw, ph);
            std::fwrite(tw.data().data(), 1, tw.size(), file_);
            std::fwrite(page_data.data(), 1, page_data.size(), file_);

            int64_t chunk_size = static_cast<int64_t>(tw.size() + page_data.size());

            ColumnChunk cc;
            cc.file_offset = data_page_offset;
            cc.meta_data.type = cs.type;
            cc.meta_data.encodings = {PLAIN, RLE};
            cc.meta_data.path_in_schema = {cs.name};
            cc.meta_data.codec = UNCOMPRESSED;
            cc.meta_data.num_values = col.num_values;
            cc.meta_data.total_uncompressed_size = chunk_size;
            cc.meta_data.total_compressed_size = chunk_size;
            cc.meta_data.data_page_offset = data_page_offset;

            rg.columns.push_back(cc);
            total_size += chunk_size;
        }

        rg.total_byte_size = total_size;
        row_groups_.push_back(rg);
    }

    void close() {
        if (closed_ || !file_) return;
        closed_ = true;

        // Build FileMetaData
        FileMetaData meta;
        meta.version = 2;
        meta.created_by = "parquet-tool-cpp";

        // Root schema element
        SchemaElement root;
        root.name = "schema";
        root.num_children = static_cast<int32_t>(schema_.size());
        meta.schema.push_back(root);

        // Column schema elements
        for (auto& cs : schema_) {
            SchemaElement se;
            se.name = cs.name;
            se.type = cs.type;
            se.repetition_type = cs.optional ? OPTIONAL : REQUIRED;
            if (cs.type == BYTE_ARRAY) {
                se.converted_type = CT_UTF8;
            }
            meta.schema.push_back(se);
        }

        int64_t total_rows = 0;
        for (auto& rg : row_groups_) total_rows += rg.num_rows;
        meta.num_rows = total_rows;
        meta.row_groups = row_groups_;

        // Serialize footer
        thrift::CompactWriter tw;
        serial::writeFileMetaData(tw, meta);

        std::fwrite(tw.data().data(), 1, tw.size(), file_);

        // Footer length (4 bytes LE)
        uint32_t footer_len = static_cast<uint32_t>(tw.size());
        std::fwrite(&footer_len, 4, 1, file_);

        // Magic
        std::fwrite("PAR1", 1, 4, file_);

        std::fclose(file_);
        file_ = nullptr;
    }
};

// ============================================================
// Reader
// ============================================================
class Reader {
    FILE* file_ = nullptr;
    FileMetaData metadata_;
    std::vector<ColSchema> col_schemas_;
    bool closed_ = false;

public:
    Reader() = default;
    ~Reader() { if (!closed_) close(); }

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    void open(const std::string& path) {
        file_ = std::fopen(path.c_str(), "rb");
        if (!file_) throw std::runtime_error("cannot open file for reading: " + path);

        // Verify magic at start
        char magic[4];
        std::fread(magic, 1, 4, file_);
        if (std::memcmp(magic, "PAR1", 4) != 0)
            throw std::runtime_error("not a valid parquet file (bad header magic)");

        // Read footer
        std::fseek(file_, -8, SEEK_END);
        uint8_t tail[8];
        std::fread(tail, 1, 8, file_);
        if (std::memcmp(tail + 4, "PAR1", 4) != 0)
            throw std::runtime_error("not a valid parquet file (bad footer magic)");

        uint32_t footer_len;
        std::memcpy(&footer_len, tail, 4);

        std::fseek(file_, -(8 + static_cast<long>(footer_len)), SEEK_END);
        std::vector<uint8_t> footer_buf(footer_len);
        std::fread(footer_buf.data(), 1, footer_len, file_);

        thrift::CompactReader tr(footer_buf.data(), footer_buf.size());
        metadata_ = serial::readFileMetaData(tr);

        // Build col_schemas from metadata (skip root element)
        for (size_t i = 1; i < metadata_.schema.size(); i++) {
            auto& se = metadata_.schema[i];
            ColSchema cs;
            cs.name = se.name;
            cs.type = se.type;
            cs.optional = (se.repetition_type == OPTIONAL);
            col_schemas_.push_back(cs);
        }
    }

    const FileMetaData& metadata() const { return metadata_; }
    const std::vector<ColSchema>& colSchemas() const { return col_schemas_; }

    std::vector<ColumnData> readRowGroup(int index) {
        if (!file_) throw std::runtime_error("reader not open");
        if (index < 0 || index >= static_cast<int>(metadata_.row_groups.size()))
            throw std::runtime_error("row group index out of range");

        auto& rg = metadata_.row_groups[index];
        std::vector<ColumnData> result;

        for (size_t ci = 0; ci < rg.columns.size(); ci++) {
            auto& cc = rg.columns[ci];
            auto& cs = col_schemas_[ci];

            // Seek to data page
            std::fseek(file_, static_cast<long>(cc.meta_data.data_page_offset), SEEK_SET);

            // Read page header + data
            // We need to read enough bytes to cover the thrift header + page data
            // Read a reasonable amount then parse
            int64_t chunk_size = cc.meta_data.total_compressed_size;
            std::vector<uint8_t> chunk_buf(static_cast<size_t>(chunk_size));
            size_t read_count = std::fread(chunk_buf.data(), 1, chunk_buf.size(), file_);
            if (read_count < chunk_buf.size()) {
                chunk_buf.resize(read_count);
            }

            thrift::CompactReader tr(chunk_buf.data(), chunk_buf.size());
            PageHeader ph = serial::readPageHeader(tr);

            int32_t num_values = ph.data_page_header.num_values;
            const uint8_t* page_data = chunk_buf.data() + tr.pos();
            size_t page_data_size = chunk_buf.size() - tr.pos();

            ColumnData cd;
            cd.type = cs.type;
            cd.num_values = num_values;
            cd.is_optional = cs.optional;

            size_t data_offset = 0;

            if (cs.optional) {
                // Decode definition levels
                size_t def_bytes_read = 0;
                cd.def_levels = encoding::decodeDefLevels(page_data, page_data_size, num_values, def_bytes_read);
                data_offset = def_bytes_read;

                // Count non-null values
                int32_t non_null = 0;
                for (int32_t i = 0; i < num_values; i++) {
                    if (cd.def_levels[i]) non_null++;
                }
                cd.num_nulls = num_values - non_null;

                // Decode only non-null values
                encoding::decodePlainValues(cs.type, page_data + data_offset,
                                            page_data_size - data_offset, non_null, cd);
            } else {
                cd.num_nulls = 0;
                cd.def_levels.assign(num_values, 1);
                encoding::decodePlainValues(cs.type, page_data + data_offset,
                                            page_data_size - data_offset, num_values, cd);
            }

            result.push_back(std::move(cd));
        }
        return result;
    }

    void close() {
        if (closed_ || !file_) return;
        closed_ = true;
        std::fclose(file_);
        file_ = nullptr;
    }
};

// ============================================================
// Utility: read metadata only
// ============================================================
inline FileMetaData readMetadataOnly(const std::string& path) {
    FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("cannot open file: " + path);

    if (std::fseek(f, -8, SEEK_END) != 0) {
        std::fclose(f);
        throw std::runtime_error("invalid parquet file (cannot seek footer): " + path);
    }

    uint8_t tail[8];
    const size_t tail_read = std::fread(tail, 1, 8, f);
    if (tail_read != 8) {
        std::fclose(f);
        throw std::runtime_error("invalid parquet file (incomplete footer): " + path);
    }

    if (std::memcmp(tail + 4, "PAR1", 4) != 0) {
        std::fclose(f);
        throw std::runtime_error("not a valid parquet file");
    }
    uint32_t footer_len;
    std::memcpy(&footer_len, tail, 4);

    if (std::fseek(f, -(8 + static_cast<long>(footer_len)), SEEK_END) != 0) {
        std::fclose(f);
        throw std::runtime_error("invalid parquet file (cannot seek metadata): " + path);
    }

    std::vector<uint8_t> buf(footer_len);
    const size_t metadata_read = std::fread(buf.data(), 1, footer_len, f);
    if (metadata_read != footer_len) {
        std::fclose(f);
        throw std::runtime_error("invalid parquet file (incomplete metadata): " + path);
    }

    std::fclose(f);

    thrift::CompactReader tr(buf.data(), buf.size());
    return serial::readFileMetaData(tr);
}

} // namespace parquet
