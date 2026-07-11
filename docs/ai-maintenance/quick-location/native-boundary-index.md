<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=CMakeLists.txt,src/lib/binding.ts,src/native/addon.cpp,src/native/parquet.h,src/native/thrift.h,scripts/install.cjs -->
# Native Boundary Index

`src/lib/binding.ts` defines the TypeScript `NativeAddon` contract. `src/native/addon.cpp` registers the functions. `parquet.h` implements file I/O and `thrift.h` implements the compact protocol. Names, arguments, handles, and return shapes must match on both sides.

Search with `rg -n "createWriter|writeRowGroup|openReader|readRowGroup|openAppender|getMetadata" src/lib/binding.ts src/native`. Check error translation, handle lifetime, and scalar conversions. Run `npm run build:native`, `npm run build:ts`, and `npm test` for native changes.
