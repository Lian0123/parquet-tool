<!-- AI-DOC: owner=maintainers; verified=2026-10-28; sources=CMakeLists.txt,src/lib/binding.ts,src/native/addon.cpp,src/native/parquet.h,src/native/thrift.h,scripts/install.cjs -->
# Native Boundary Index

`src/lib/binding.ts` defines the TypeScript `NativeAddon` contract. `src/native/addon.cpp` registers the functions. `parquet.h` implements file I/O and `thrift.h` implements the compact protocol. Names, arguments, handles, and return shapes must match on both sides.

Search with `rg -n "createWriter|writeRowGroup|openReader|readRowGroup|openAppender|getMetadata" src/lib/binding.ts src/native`. Check error translation, handle lifetime, and scalar conversions. Run `npm run build:native`, `npm run build:ts`, and `npm test` for native changes.

`src/wasm/browser.cpp` binds the same header implementation for the static browser
converter. Changes to either shared header require `npm run build:wasm --prefix
tools/site-converter` with Emscripten 3.1.74, followed by website tests. The source
hash manifest guards stale checked-in WASM. Standard SchemaElement field IDs are
written; both standard and pre-fix project field layouts are read. Fixtures cover
legacy data, independent PyArrow PLAIN data, and unsupported encodings.
