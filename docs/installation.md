# Installation

```bash
# clone repository
git clone <repo-url> parquet-tool
cd parquet-tool

# install dependencies and build
npm install
npm run build

# build only native addon
npm run build:native

# run tests
npm test

# lint source
npm run lint
```

Node.js 18+ is required. The native addon is compiled with CMake; install
CMake via your package manager (e.g. `brew install cmake`).
