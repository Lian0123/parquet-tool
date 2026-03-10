/**
 * N-API addon binding for the Parquet reader/writer.
 * Exposes createWriter, writeRowGroup, closeWriter,
 *         openReader, readRowGroup, closeReader,
 *         openAppender, closeAppender, getMetadata
 * to JavaScript.
 */

#include <napi.h>
#include "parquet.h"

#include <memory>
#include <mutex>
#include <unordered_map>

// ============================================================
// Handle management
// ============================================================
static std::mutex g_mutex;
static int g_next_handle = 1;
static std::unordered_map<int, std::unique_ptr<parquet::Writer>> g_writers;
static std::unordered_map<int, std::unique_ptr<parquet::Reader>> g_readers;

// ============================================================
// Helpers: JS ↔ C++ conversion
// ============================================================

static std::vector<parquet::ColSchema> parseSchema(Napi::Env env, Napi::Array arr) {
    std::vector<parquet::ColSchema> schema;
    for (uint32_t i = 0; i < arr.Length(); i++) {
        auto obj = arr.Get(i).As<Napi::Object>();
        parquet::ColSchema cs;
        cs.name = obj.Get("name").As<Napi::String>().Utf8Value();
        cs.type = obj.Get("type").As<Napi::Number>().Int32Value();
        cs.optional = false;
        if (obj.Has("optional")) {
            cs.optional = obj.Get("optional").As<Napi::Boolean>().Value();
        }
        schema.push_back(cs);
    }
    return schema;
}

static parquet::ColumnData extractColumn(Napi::Env env, Napi::Array values,
                                         const parquet::ColSchema& cs) {
    parquet::ColumnData cd;
    cd.type = cs.type;
    cd.num_values = static_cast<int32_t>(values.Length());
    cd.is_optional = cs.optional;
    cd.num_nulls = 0;

    if (cs.optional) {
        cd.def_levels.resize(cd.num_values, 1);
    }

    for (uint32_t i = 0; i < values.Length(); i++) {
        Napi::Value val = values.Get(i);
        bool is_null = val.IsNull() || val.IsUndefined();

        if (cs.optional) {
            if (is_null) {
                cd.def_levels[i] = 0;
                cd.num_nulls++;
                continue;
            }
        }

        switch (cs.type) {
        case parquet::BOOLEAN:
            cd.bool_vals.push_back(val.As<Napi::Boolean>().Value() ? 1 : 0);
            break;
        case parquet::INT32:
            cd.int32_vals.push_back(val.As<Napi::Number>().Int32Value());
            break;
        case parquet::INT64:
            if (val.IsBigInt()) {
                bool lossless;
                cd.int64_vals.push_back(val.As<Napi::BigInt>().Int64Value(&lossless));
            } else {
                cd.int64_vals.push_back(static_cast<int64_t>(val.As<Napi::Number>().DoubleValue()));
            }
            break;
        case parquet::FLOAT:
            cd.float_vals.push_back(static_cast<float>(val.As<Napi::Number>().FloatValue()));
            break;
        case parquet::DOUBLE:
            cd.double_vals.push_back(val.As<Napi::Number>().DoubleValue());
            break;
        case parquet::BYTE_ARRAY:
            cd.string_vals.push_back(val.As<Napi::String>().Utf8Value());
            break;
        default:
            Napi::Error::New(env, "unsupported column type").ThrowAsJavaScriptException();
            return cd;
        }
    }
    return cd;
}

static Napi::Array columnDataToJS(Napi::Env env, const parquet::ColumnData& cd,
                                   const parquet::ColSchema& cs) {
    auto arr = Napi::Array::New(env, cd.num_values);
    int32_t val_idx = 0;

    for (int32_t i = 0; i < cd.num_values; i++) {
        if (cs.optional && cd.def_levels[i] == 0) {
            arr.Set(i, env.Null());
            continue;
        }

        switch (cs.type) {
        case parquet::BOOLEAN:
            arr.Set(i, Napi::Boolean::New(env, cd.bool_vals[val_idx] != 0));
            break;
        case parquet::INT32:
            arr.Set(i, Napi::Number::New(env, cd.int32_vals[val_idx]));
            break;
        case parquet::INT64:
            arr.Set(i, Napi::BigInt::New(env, cd.int64_vals[val_idx]));
            break;
        case parquet::FLOAT:
            arr.Set(i, Napi::Number::New(env, cd.float_vals[val_idx]));
            break;
        case parquet::DOUBLE:
            arr.Set(i, Napi::Number::New(env, cd.double_vals[val_idx]));
            break;
        case parquet::BYTE_ARRAY:
            arr.Set(i, Napi::String::New(env, cd.string_vals[val_idx]));
            break;
        default:
            arr.Set(i, env.Null());
            break;
        }
        val_idx++;
    }
    return arr;
}

static Napi::Object metadataToJS(Napi::Env env, const parquet::FileMetaData& meta,
                                  const std::vector<parquet::ColSchema>& schemas) {
    auto obj = Napi::Object::New(env);
    obj.Set("version", Napi::Number::New(env, meta.version));
    obj.Set("numRows", Napi::Number::New(env, static_cast<double>(meta.num_rows)));
    obj.Set("numRowGroups", Napi::Number::New(env, static_cast<double>(meta.row_groups.size())));
    obj.Set("createdBy", Napi::String::New(env, meta.created_by));

    // Schema
    auto schemaArr = Napi::Array::New(env, schemas.size());
    for (size_t i = 0; i < schemas.size(); i++) {
        auto col = Napi::Object::New(env);
        col.Set("name", Napi::String::New(env, schemas[i].name));
        col.Set("type", Napi::Number::New(env, schemas[i].type));
        col.Set("optional", Napi::Boolean::New(env, schemas[i].optional));
        schemaArr.Set(static_cast<uint32_t>(i), col);
    }
    obj.Set("schema", schemaArr);

    // Row groups
    auto rgArr = Napi::Array::New(env, meta.row_groups.size());
    for (size_t i = 0; i < meta.row_groups.size(); i++) {
        auto& rg = meta.row_groups[i];
        auto rgObj = Napi::Object::New(env);
        rgObj.Set("numRows", Napi::Number::New(env, static_cast<double>(rg.num_rows)));
        rgObj.Set("totalByteSize", Napi::Number::New(env, static_cast<double>(rg.total_byte_size)));

        auto colArr = Napi::Array::New(env, rg.columns.size());
        for (size_t j = 0; j < rg.columns.size(); j++) {
            auto& cc = rg.columns[j];
            auto ccObj = Napi::Object::New(env);
            if (!cc.meta_data.path_in_schema.empty())
                ccObj.Set("name", Napi::String::New(env, cc.meta_data.path_in_schema[0]));
            ccObj.Set("type", Napi::Number::New(env, cc.meta_data.type));
            ccObj.Set("numValues", Napi::Number::New(env, static_cast<double>(cc.meta_data.num_values)));
            ccObj.Set("compressedSize", Napi::Number::New(env, static_cast<double>(cc.meta_data.total_compressed_size)));
            ccObj.Set("uncompressedSize", Napi::Number::New(env, static_cast<double>(cc.meta_data.total_uncompressed_size)));
            colArr.Set(static_cast<uint32_t>(j), ccObj);
        }
        rgObj.Set("columns", colArr);
        rgArr.Set(static_cast<uint32_t>(i), rgObj);
    }
    obj.Set("rowGroups", rgArr);
    return obj;
}

// ============================================================
// N-API exports
// ============================================================

// createWriter(filePath: string, schema: SchemaColumn[]): number
static Napi::Value CreateWriter(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsArray()) {
        Napi::TypeError::New(env, "Expected (string, array)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string path = info[0].As<Napi::String>().Utf8Value();
    auto schema = parseSchema(env, info[1].As<Napi::Array>());

    auto writer = std::make_unique<parquet::Writer>();
    try {
        writer->open(path, schema);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::lock_guard<std::mutex> lock(g_mutex);
    int handle = g_next_handle++;
    g_writers[handle] = std::move(writer);
    return Napi::Number::New(env, handle);
}

// writeRowGroup(handle: number, columns: Array<{values: any[]}>): void
static Napi::Value WriteRowGroup(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2) {
        Napi::TypeError::New(env, "Expected (number, array)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    int handle = info[0].As<Napi::Number>().Int32Value();
    auto colArr = info[1].As<Napi::Array>();

    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_writers.find(handle);
    if (it == g_writers.end()) {
        Napi::Error::New(env, "Invalid writer handle").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    // We need the schema to know column types
    // Retrieve it via a metadata query — actually the writer stores it internally.
    // We need to pass the schema through. Let's store it alongside the writer.
    // For simplicity, store schemas in a separate map.
    // Actually the writer already has schema_ but it's private. Let's use a parallel map.

    // Alternative: pass schema info from JS side. The columns array elements have values.
    // We stored schema in a companion map.
    Napi::Error::New(env, "use writeRowGroupWithSchema instead").ThrowAsJavaScriptException();
    return env.Undefined();
}

// Store schemas alongside writers
static std::unordered_map<int, std::vector<parquet::ColSchema>> g_writer_schemas;

// createWriter2 that also stores schema
static Napi::Value CreateWriter2(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsArray()) {
        Napi::TypeError::New(env, "Expected (string, array)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string path = info[0].As<Napi::String>().Utf8Value();
    auto schema = parseSchema(env, info[1].As<Napi::Array>());

    auto writer = std::make_unique<parquet::Writer>();
    try {
        writer->open(path, schema);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::lock_guard<std::mutex> lock(g_mutex);
    int handle = g_next_handle++;
    g_writers[handle] = std::move(writer);
    g_writer_schemas[handle] = schema;
    return Napi::Number::New(env, handle);
}

// writeRowGroup2(handle: number, columns: Array<{values: any[]}>): void
static Napi::Value WriteRowGroup2(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (info.Length() < 2) {
        Napi::TypeError::New(env, "Expected (number, array)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    int handle = info[0].As<Napi::Number>().Int32Value();
    auto colArr = info[1].As<Napi::Array>();

    std::lock_guard<std::mutex> lock(g_mutex);
    auto wit = g_writers.find(handle);
    if (wit == g_writers.end()) {
        Napi::Error::New(env, "Invalid writer handle").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    auto sit = g_writer_schemas.find(handle);
    if (sit == g_writer_schemas.end()) {
        Napi::Error::New(env, "No schema for writer").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    auto& schema = sit->second;

    if (colArr.Length() != schema.size()) {
        Napi::Error::New(env, "Column count mismatch").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::vector<parquet::ColumnData> columns;
    for (uint32_t i = 0; i < colArr.Length(); i++) {
        auto colObj = colArr.Get(i).As<Napi::Object>();
        auto values = colObj.Get("values").As<Napi::Array>();
        columns.push_back(extractColumn(env, values, schema[i]));
    }

    try {
        wit->second->writeRowGroup(columns);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

// closeWriter(handle: number): void
static Napi::Value CloseWriter(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int handle = info[0].As<Napi::Number>().Int32Value();

    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_writers.find(handle);
    if (it != g_writers.end()) {
        try {
            it->second->close();
        } catch (const std::exception& e) {
            Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        }
        g_writers.erase(it);
        g_writer_schemas.erase(handle);
    }
    return env.Undefined();
}

// openReader(filePath: string): {handle: number, metadata: object}
static Napi::Value OpenReader(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string path = info[0].As<Napi::String>().Utf8Value();

    auto reader = std::make_unique<parquet::Reader>();
    try {
        reader->open(path);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto& meta = reader->metadata();
    auto schemas = reader->colSchemas();

    std::lock_guard<std::mutex> lock(g_mutex);
    int handle = g_next_handle++;
    auto result = Napi::Object::New(env);
    result.Set("handle", Napi::Number::New(env, handle));
    result.Set("metadata", metadataToJS(env, meta, schemas));

    g_readers[handle] = std::move(reader);
    return result;
}

// readRowGroup(handle: number, index: number): {numRows: number, columns: {[name]: any[]}}
static Napi::Value ReadRowGroup(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int handle = info[0].As<Napi::Number>().Int32Value();
    int index = info[1].As<Napi::Number>().Int32Value();

    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_readers.find(handle);
    if (it == g_readers.end()) {
        Napi::Error::New(env, "Invalid reader handle").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto& reader = it->second;
    auto schemas = reader->colSchemas();

    std::vector<parquet::ColumnData> columns;
    try {
        columns = reader->readRowGroup(index);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto result = Napi::Object::New(env);
    int32_t numRows = 0;
    if (!columns.empty()) numRows = columns[0].num_values;
    result.Set("numRows", Napi::Number::New(env, numRows));

    auto colsObj = Napi::Object::New(env);
    for (size_t i = 0; i < columns.size(); i++) {
        colsObj.Set(schemas[i].name, columnDataToJS(env, columns[i], schemas[i]));
    }
    result.Set("columns", colsObj);
    return result;
}

// closeReader(handle: number): void
static Napi::Value CloseReader(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int handle = info[0].As<Napi::Number>().Int32Value();

    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_readers.find(handle);
    if (it != g_readers.end()) {
        it->second->close();
        g_readers.erase(it);
    }
    return env.Undefined();
}

// getMetadata(filePath: string): object
static Napi::Value GetMetadata(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string path = info[0].As<Napi::String>().Utf8Value();

    try {
        auto meta = parquet::readMetadataOnly(path);
        std::vector<parquet::ColSchema> schemas;
        for (size_t i = 1; i < meta.schema.size(); i++) {
            parquet::ColSchema cs;
            cs.name = meta.schema[i].name;
            cs.type = meta.schema[i].type;
            cs.optional = (meta.schema[i].repetition_type == parquet::OPTIONAL);
            schemas.push_back(cs);
        }
        return metadataToJS(env, meta, schemas);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }
}

// openAppender(filePath: string): {handle: number, metadata: object}
static Napi::Value OpenAppender(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string path = info[0].As<Napi::String>().Utf8Value();

    auto writer = std::make_unique<parquet::Writer>();
    try {
        writer->openForAppend(path);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    // Read metadata to return
    parquet::FileMetaData meta;
    std::vector<parquet::ColSchema> schemas;
    try {
        meta = parquet::readMetadataOnly(path);
        for (size_t i = 1; i < meta.schema.size(); i++) {
            parquet::ColSchema cs;
            cs.name = meta.schema[i].name;
            cs.type = meta.schema[i].type;
            cs.optional = (meta.schema[i].repetition_type == parquet::OPTIONAL);
            schemas.push_back(cs);
        }
    } catch (...) {
        // metadata already truncated, get from writer's stored schema
    }

    std::lock_guard<std::mutex> lock(g_mutex);
    int handle = g_next_handle++;
    g_writers[handle] = std::move(writer);

    // We need to build schemas from the writer - but it's private.
    // Since openForAppend reads the schema, let's store it.
    // Actually we can re-read before truncation. Let's fix: read meta before openForAppend.
    // For now, return a minimal result.

    auto result = Napi::Object::New(env);
    result.Set("handle", Napi::Number::New(env, handle));

    if (!schemas.empty()) {
        g_writer_schemas[handle] = schemas;
        result.Set("metadata", metadataToJS(env, meta, schemas));
    }
    return result;
}

// openAppender2: reads metadata first, then opens for append
static Napi::Value OpenAppender2(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string path = info[0].As<Napi::String>().Utf8Value();

    // Read metadata before truncation
    parquet::FileMetaData meta;
    std::vector<parquet::ColSchema> schemas;
    try {
        meta = parquet::readMetadataOnly(path);
        for (size_t i = 1; i < meta.schema.size(); i++) {
            parquet::ColSchema cs;
            cs.name = meta.schema[i].name;
            cs.type = meta.schema[i].type;
            cs.optional = (meta.schema[i].repetition_type == parquet::OPTIONAL);
            schemas.push_back(cs);
        }
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    auto writer = std::make_unique<parquet::Writer>();
    try {
        writer->openForAppend(path);
    } catch (const std::exception& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::lock_guard<std::mutex> lock(g_mutex);
    int handle = g_next_handle++;
    g_writers[handle] = std::move(writer);
    g_writer_schemas[handle] = schemas;

    auto result = Napi::Object::New(env);
    result.Set("handle", Napi::Number::New(env, handle));
    result.Set("metadata", metadataToJS(env, meta, schemas));
    return result;
}

// ============================================================
// Module init
// ============================================================
static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    exports.Set("createWriter", Napi::Function::New(env, CreateWriter2));
    exports.Set("writeRowGroup", Napi::Function::New(env, WriteRowGroup2));
    exports.Set("closeWriter", Napi::Function::New(env, CloseWriter));
    exports.Set("openReader", Napi::Function::New(env, OpenReader));
    exports.Set("readRowGroup", Napi::Function::New(env, ReadRowGroup));
    exports.Set("closeReader", Napi::Function::New(env, CloseReader));
    exports.Set("getMetadata", Napi::Function::New(env, GetMetadata));
    exports.Set("openAppender", Napi::Function::New(env, OpenAppender2));
    return exports;
}

NODE_API_MODULE(parquet_addon, Init)
