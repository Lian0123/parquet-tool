<!-- AI-DOC: owner=maintainers; verified=2026-07-11; sources=viewer/app.py,viewer/templates/index.html,viewer/requirements.txt,viewer/Dockerfile,docker-compose.yml -->
# Viewer Index

The viewer is independent of the TypeScript library and native addon: Flask route -> PyArrow -> Pandas/HTML or JSON.

Routes and file listing are in `viewer/app.py`; HTML is in `viewer/templates/index.html`; dependencies and container settings are in `viewer/requirements.txt`, `viewer/Dockerfile`, and `docker-compose.yml`.

Keep `file` constrained to `PARQUET_DIR`, validate limits, and avoid unbounded reads. After changes, run Python syntax checks, build the container, and smoke-test `/`, `/view`, `/api/files`, and `/api/read`.
