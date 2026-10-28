// Browser binding for the same header-only implementation used by the Node addon.
#include <emscripten/bind.h>
#include "../native/parquet.h"
#include <limits>

using emscripten::val;

static val failure(const std::exception& error) {
    val result = val::object(); result.set("error", std::string(error.what())); return result;
}

static val writeColumns(val columns) {
    try {
        const unsigned count = columns["length"].as<unsigned>();
        if (!count || count > 200) throw std::runtime_error("invalid column count");
        std::vector<parquet::ColSchema> schema;
        std::vector<parquet::ColumnData> data;
        unsigned rows = 0;
        for (unsigned i = 0; i < count; i++) {
            val input = columns[i];
            const std::string type = input["type"].as<std::string>();
            int physical = type == "BOOLEAN" ? parquet::BOOLEAN : type == "INT32" ? parquet::INT32 :
                type == "INT64" ? parquet::INT64 : type == "FLOAT" ? parquet::FLOAT :
                type == "DOUBLE" ? parquet::DOUBLE : type == "STRING" ? parquet::BYTE_ARRAY : -1;
            if (physical < 0) throw std::runtime_error("unsupported column type");
            schema.push_back({ input["name"].as<std::string>(), physical, true });
            val values = input["data"];
            const unsigned length = values["length"].as<unsigned>();
            if (!i) rows = length;
            if (rows != length || rows > 100000 || static_cast<uint64_t>(rows) * count > 2000000)
                throw std::runtime_error("invalid row count or browser table limit exceeded");
            parquet::ColumnData column{};
            column.type = physical; column.num_values = length; column.is_optional = true;
            for (unsigned row = 0; row < length; row++) {
                val value = values[row];
                const bool missing = value.isNull() || value.isUndefined();
                column.def_levels.push_back(missing ? 0 : 1);
                if (missing) { column.num_nulls++; continue; }
                switch (physical) {
                case parquet::BOOLEAN: column.bool_vals.push_back(value.as<bool>() ? 1 : 0); break;
                case parquet::INT32: column.int32_vals.push_back(value.as<int32_t>()); break;
                case parquet::INT64: column.int64_vals.push_back(std::stoll(val::global("String")(value).as<std::string>())); break;
                case parquet::FLOAT: column.float_vals.push_back(value.as<float>()); break;
                case parquet::DOUBLE: column.double_vals.push_back(value.as<double>()); break;
                case parquet::BYTE_ARRAY: column.string_vals.push_back(value.as<std::string>()); break;
                }
            }
            data.push_back(std::move(column));
        }
        parquet::Writer writer;
        writer.open("/output.parquet", schema);
        writer.writeRowGroup(data);
        writer.close();
        return val::object();
    } catch (const std::exception& error) { return failure(error); }
}

static val readRows() {
    try {
        parquet::Reader reader; reader.open("/input.parquet");
        const auto& metadata = reader.metadata();
        const auto& schema = reader.colSchemas();
        for (const auto& column : schema)
            if (column.name.empty() || column.name == "__proto__" || column.name == "constructor" || column.name == "prototype")
                throw std::runtime_error("invalid column name");
        if (metadata.num_rows < 0 || metadata.num_rows > 100000 || schema.size() > 200 ||
            static_cast<uint64_t>(metadata.num_rows) * schema.size() > 2000000)
            throw std::runtime_error("browser table limit exceeded");
        val rows = val::array();
        for (size_t group = 0; group < metadata.row_groups.size(); group++) {
            auto columns = reader.readRowGroup(group);
            const auto count = metadata.row_groups[group].num_rows;
            std::vector<size_t> positions(schema.size(), 0);
            for (int64_t row = 0; row < count; row++) {
                val item = val::object();
                for (size_t c = 0; c < schema.size(); c++) {
                    auto& column = columns[c];
                    val value = val::null();
                    if (!column.is_optional || column.def_levels.at(static_cast<size_t>(row))) {
                        const auto at = positions[c]++;
                        switch (column.type) {
                        case parquet::BOOLEAN: value = val(column.bool_vals.at(at) != 0); break;
                        case parquet::INT32: value = val(column.int32_vals.at(at)); break;
                        case parquet::INT64: value = val::global("BigInt")(std::to_string(column.int64_vals.at(at))); break;
                        case parquet::FLOAT: value = val(column.float_vals.at(at)); break;
                        case parquet::DOUBLE: value = val(column.double_vals.at(at)); break;
                        case parquet::BYTE_ARRAY: value = val(column.string_vals.at(at)); break;
                        default: throw std::runtime_error("unsupported Parquet physical type");
                        }
                    }
                    item.set(schema[c].name, value);
                }
                rows.call<void>("push", item);
            }
        }
        reader.close();
        val result = val::object(); result.set("rows", rows); return result;
    } catch (const std::exception& error) { return failure(error); }
}

EMSCRIPTEN_BINDINGS(parquet_project) {
    emscripten::function("writeColumns", &writeColumns);
    emscripten::function("readRows", &readRows);
}
