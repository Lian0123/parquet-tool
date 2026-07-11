<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=package.json,src/lib/index.ts,src/cli/index.ts,src/native/addon.cpp,viewer/app.py -->
# AI Maintenance Entry

This is the only first stop for AI agents and maintainers. It is designed to locate the correct source, tests, and validation commands with minimal token use.

## 30-second summary

- `parquet-tool` is a Node.js 18+ TypeScript library and CLI. Core Parquet I/O is implemented by a C++ Node-API addon.
- Library exports: `src/lib/index.ts`; CLI: `src/cli/index.ts`; native boundary: `src/lib/binding.ts` and `src/native/addon.cpp`.
- Features include read, write, append, split, merge, validate, CSV/Arrow/JSON/XML/Buffer conversion, and row-group parallel processing.
- `viewer/` is a separate Flask + PyArrow development tool, not part of the npm runtime path.
- Authority order: executed results > source/configuration > tests > maintenance docs > README/CHANGELOG.

## Task routing

| Task | Read first | Then, if needed |
|---|---|---|
| Understand the project | [Project facts](project-memory/project-facts.md) | [Feature index](quick-location/feature-index.md) |
| Change Reader, Writer, Schema, or types | [Core read/write](quick-location/core-read-write-index.md) | [Native boundary](quick-location/native-boundary-index.md) |
| Change C++, builds, or binary loading | [Native boundary](quick-location/native-boundary-index.md) | [Build and installation](build-installation-index.md), [Risks](project-memory/risks-and-tech-debt.md) |
| Change conversion or Buffer logic | [Conversion and processing](quick-location/conversion-processing-index.md) | [Core read/write](quick-location/core-read-write-index.md) |
| Change split, merge, validate, or parallel logic | [Conversion and processing](quick-location/conversion-processing-index.md) | [Testing and quality](testing-quality-index.md) |
| Change the CLI | [CLI index](quick-location/cli-index.md) | The relevant feature index |
| Change the viewer | [Viewer index](quick-location/viewer-index.md) | [Risks](project-memory/risks-and-tech-debt.md) |
| Fix a bug or add a feature | [Task workflow](task-handling/task-workflow.md) | [Change decisions](decision-making/change-decision-process.md) |
| Test, release, or change CI | [Testing and quality](testing-quality-index.md) | [Build and installation](build-installation-index.md) |
| Update living documentation | [Documentation protocol](documentation-protocol.md) | [Progressive reading](reading-strategy/progressive-reading.md) |

## Fixed loop

1. Choose one route above; read only one index first.
2. Use its search terms to verify symbols and paths with `rg`.
3. Define the observable goal, scope, and risks before editing.
4. Run the relevant checks and inspect the diff.
5. Update affected living docs and run `npm run docs:check`.

Stop reading when you know the entry point, data flow, files to change, risks, and validation commands.
