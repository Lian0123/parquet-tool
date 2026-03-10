#pragma once
/**
 * Thrift Compact Protocol implementation for Parquet metadata serialization.
 * Implements only the subset needed for Parquet file format.
 */

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace thrift {

// Compact protocol type IDs
enum CompactType : uint8_t {
    CT_STOP = 0,
    CT_BOOLEAN_TRUE = 1,
    CT_BOOLEAN_FALSE = 2,
    CT_BYTE = 3,
    CT_I16 = 4,
    CT_I32 = 5,
    CT_I64 = 6,
    CT_DOUBLE = 7,
    CT_BINARY = 8,
    CT_LIST = 9,
    CT_SET = 10,
    CT_MAP = 11,
    CT_STRUCT = 12,
};

// ============================================================
// Writer
// ============================================================
class CompactWriter {
    std::vector<uint8_t> buf_;
    std::vector<int16_t> field_stack_;
    int16_t last_field_id_ = 0;

public:
    const std::vector<uint8_t>& data() const { return buf_; }
    size_t size() const { return buf_.size(); }

    void writeByte(uint8_t b) { buf_.push_back(b); }

    void writeBytes(const uint8_t* p, size_t n) {
        buf_.insert(buf_.end(), p, p + n);
    }

    void writeVarint(uint64_t n) {
        while (n >= 0x80) {
            buf_.push_back(static_cast<uint8_t>((n & 0x7F) | 0x80));
            n >>= 7;
        }
        buf_.push_back(static_cast<uint8_t>(n));
    }

    void writeZigzagI32(int32_t n) {
        writeVarint(static_cast<uint32_t>((n << 1) ^ (n >> 31)));
    }

    void writeZigzagI64(int64_t n) {
        writeVarint(static_cast<uint64_t>((n << 1) ^ (n >> 63)));
    }

    void writeBinary(const std::string& s) {
        writeVarint(s.size());
        writeBytes(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }

    void writeDouble(double d) {
        uint8_t tmp[8];
        std::memcpy(tmp, &d, 8);
        writeBytes(tmp, 8);
    }

    // --- Struct fields ---
    void writeFieldHeader(CompactType type, int16_t field_id) {
        int16_t delta = field_id - last_field_id_;
        if (delta > 0 && delta <= 15) {
            writeByte(static_cast<uint8_t>((delta << 4) | type));
        } else {
            writeByte(static_cast<uint8_t>(type));
            writeZigzagI32(field_id);
        }
        last_field_id_ = field_id;
    }

    void writeFieldBool(int16_t fid, bool v) {
        writeFieldHeader(v ? CT_BOOLEAN_TRUE : CT_BOOLEAN_FALSE, fid);
    }
    void writeFieldI32(int16_t fid, int32_t v) {
        writeFieldHeader(CT_I32, fid);
        writeZigzagI32(v);
    }
    void writeFieldI64(int16_t fid, int64_t v) {
        writeFieldHeader(CT_I64, fid);
        writeZigzagI64(v);
    }
    void writeFieldDouble(int16_t fid, double v) {
        writeFieldHeader(CT_DOUBLE, fid);
        writeDouble(v);
    }
    void writeFieldString(int16_t fid, const std::string& v) {
        writeFieldHeader(CT_BINARY, fid);
        writeBinary(v);
    }

    void writeFieldListBegin(int16_t fid, CompactType elem, int32_t count) {
        writeFieldHeader(CT_LIST, fid);
        writeListHeader(elem, count);
    }

    void writeListHeader(CompactType elem, int32_t count) {
        if (count < 15) {
            writeByte(static_cast<uint8_t>((count << 4) | elem));
        } else {
            writeByte(static_cast<uint8_t>(0xF0 | elem));
            writeVarint(static_cast<uint64_t>(count));
        }
    }

    void writeFieldStructBegin(int16_t fid) {
        writeFieldHeader(CT_STRUCT, fid);
        pushStruct();
    }

    void pushStruct() {
        field_stack_.push_back(last_field_id_);
        last_field_id_ = 0;
    }
    void popStruct() {
        writeByte(CT_STOP);
        last_field_id_ = field_stack_.back();
        field_stack_.pop_back();
    }
    void writeStop() { writeByte(CT_STOP); }
};

// ============================================================
// Reader
// ============================================================
class CompactReader {
    const uint8_t* data_;
    size_t size_;
    size_t pos_ = 0;
    std::vector<int16_t> field_stack_;
    int16_t last_field_id_ = 0;

public:
    CompactReader(const uint8_t* data, size_t size)
        : data_(data), size_(size) {}

    size_t pos() const { return pos_; }
    size_t remaining() const { return size_ - pos_; }

    uint8_t readByte() {
        if (pos_ >= size_) throw std::runtime_error("thrift: unexpected end of data");
        return data_[pos_++];
    }

    void readBytes(uint8_t* out, size_t n) {
        if (pos_ + n > size_) throw std::runtime_error("thrift: unexpected end of data");
        std::memcpy(out, data_ + pos_, n);
        pos_ += n;
    }

    uint64_t readVarint() {
        uint64_t result = 0;
        int shift = 0;
        for (;;) {
            uint8_t b = readByte();
            result |= static_cast<uint64_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift >= 64) throw std::runtime_error("thrift: varint too long");
        }
    }

    int32_t readZigzagI32() {
        uint32_t n = static_cast<uint32_t>(readVarint());
        return static_cast<int32_t>((n >> 1) ^ (~(n & 1) + 1));
    }

    int64_t readZigzagI64() {
        uint64_t n = readVarint();
        return static_cast<int64_t>((n >> 1) ^ (~(n & 1) + 1));
    }

    double readDouble() {
        double d;
        readBytes(reinterpret_cast<uint8_t*>(&d), 8);
        return d;
    }

    std::string readBinary() {
        uint64_t len = readVarint();
        if (pos_ + len > size_) throw std::runtime_error("thrift: unexpected end of data");
        std::string s(reinterpret_cast<const char*>(data_ + pos_), static_cast<size_t>(len));
        pos_ += static_cast<size_t>(len);
        return s;
    }

    // --- Struct fields ---
    struct FieldHeader {
        uint8_t type;   // CompactType, 0 = STOP
        int16_t field_id;
    };

    FieldHeader readFieldHeader() {
        uint8_t byte = readByte();
        if (byte == 0) return {0, 0};

        uint8_t type = byte & 0x0F;
        uint8_t delta = (byte >> 4) & 0x0F;

        int16_t fid;
        if (delta != 0) {
            fid = last_field_id_ + delta;
        } else {
            fid = static_cast<int16_t>(readZigzagI32());
        }
        last_field_id_ = fid;
        return {type, fid};
    }

    struct ListHeader {
        uint8_t elemType;
        int32_t count;
    };

    ListHeader readListHeader() {
        uint8_t byte = readByte();
        uint8_t etype = byte & 0x0F;
        int32_t count = (byte >> 4) & 0x0F;
        if (count == 15) {
            count = static_cast<int32_t>(readVarint());
        }
        return {etype, count};
    }

    bool readBoolFromType(uint8_t type) {
        return type == CT_BOOLEAN_TRUE;
    }

    void pushStruct() {
        field_stack_.push_back(last_field_id_);
        last_field_id_ = 0;
    }
    void popStruct() {
        last_field_id_ = field_stack_.back();
        field_stack_.pop_back();
    }

    // Skip a value of the given compact type
    void skip(uint8_t type) {
        switch (type) {
        case CT_BOOLEAN_TRUE:
        case CT_BOOLEAN_FALSE:
            break;
        case CT_BYTE:
            readByte();
            break;
        case CT_I16:
        case CT_I32:
            readZigzagI32();
            break;
        case CT_I64:
            readZigzagI64();
            break;
        case CT_DOUBLE:
            pos_ += 8;
            break;
        case CT_BINARY:
            readBinary();
            break;
        case CT_LIST:
        case CT_SET: {
            auto lh = readListHeader();
            for (int32_t i = 0; i < lh.count; i++) skip(lh.elemType);
            break;
        }
        case CT_MAP: {
            auto count = static_cast<int32_t>(readVarint());
            if (count > 0) {
                uint8_t kv = readByte();
                uint8_t kt = (kv >> 4) & 0x0F;
                uint8_t vt = kv & 0x0F;
                for (int32_t i = 0; i < count; i++) {
                    skip(kt);
                    skip(vt);
                }
            }
            break;
        }
        case CT_STRUCT: {
            pushStruct();
            for (;;) {
                auto fh = readFieldHeader();
                if (fh.type == CT_STOP) break;
                skip(fh.type);
            }
            popStruct();
            break;
        }
        default:
            throw std::runtime_error("thrift: unknown type to skip");
        }
    }
};

} // namespace thrift
