# Add-on: Run the Pipeline with Docker

> **Status:** Planned, not implemented yet. Pick this up when there is time.

## Goal

Run the whole pipeline (download → clean → dbt build) inside a container with one command, so it works the same on any computer without installing Python, uv or dbt locally.

## Effort

About 1–2 hours. You add four files and change no Python or dbt code.

## Checklist

- [ ] Run `uv lock` and commit `uv.lock` so it matches `pyproject.toml`
- [ ] Add `Dockerfile`
- [ ] Add `.dockerignore`
- [ ] Add `nyc_taxi/profiles.yml`
- [ ] Add `docker-compose.yml` (optional but recommended)
- [ ] Build and run once; check that `data/nyc_taxi.duckdb` is created on the host
- [ ] Add a "Run with Docker" section to `README.md`
- [ ] Tick the Docker item under **Planned Add-ons** in `README.md`

---

## Step 1 – `Dockerfile` (project root)

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Install dependencies first so Docker can cache this layer
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --no-dev

# Copy the project and install it (adds taxi-run, taxi-download, taxi-clean)
COPY . .
RUN uv sync --locked --no-dev

# Install dbt_utils at build time so the container doesn't need it at run time
RUN cd nyc_taxi && uv run dbt deps

CMD ["uv", "run", "taxi-run"]
```

## Step 2 – `.dockerignore` (project root)

Keeps data and local files out of the image.

```
.venv/
.git/
data/
logs/
nyc_taxi/target/
nyc_taxi/dbt_packages/
nyc_taxi/logs/
*.duckdb
**/__pycache__/
*.egg-info/
```

## Step 3 – `nyc_taxi/profiles.yml`

The container has no `~/.dbt` folder, so the profile has to be part of the project. dbt finds it automatically when it runs inside `nyc_taxi/`. This also helps anyone running the pipeline without Docker.

```yaml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH', '../data/nyc_taxi.duckdb') }}"
      threads: 4
```

## Step 4 – `docker-compose.yml` (project root, optional)

Mounts the local `data/` folder so the downloaded files and the DuckDB database stay on your computer.

```yaml
services:
  pipeline:
    build: .
    volumes:
      - ./data:/app/data
    command: ["uv", "run", "taxi-run"]
```

## Step 5 – Build and run

```bash
docker compose build
docker compose run --rm pipeline                                     # full pipeline
docker compose run --rm pipeline uv run taxi-run -y 2025 -m 1        # one month
docker compose run --rm pipeline sh -c "cd nyc_taxi && uv run dbt test"
```

Without compose:

```bash
docker build -t taxi-pipeline .
docker run --rm -v "%cd%/data:/app/data" taxi-pipeline        # Windows cmd
docker run --rm -v "${PWD}/data:/app/data" taxi-pipeline      # PowerShell / Mac / Linux
```

## Why the paths work without code changes

- **`config.yaml` paths:** `config.py` resolves them from `/app`, so `data/green_taxi/...` becomes `/app/data/...`, which is the mounted folder.
- **`TAXI_CLEAN_DIR`:** `taxi-run` sets it from `paths.clean` in `config.yaml`, so dbt finds the clean Parquet files inside the container too.

## Troubleshooting

| Problem | Fix |
|---|---|
| Build fails at `uv sync --locked` | `uv.lock` is out of date. Run `uv lock` locally and commit it. |
| Run fails with a DuckDB lock error | Only one writer is allowed. Close notebooks that have `nyc_taxi.duckdb` open. |
| No files appear in `data/` on the host | Check the `-v` / `volumes` mount path. |

## Possible next step

Add a Prefect server as a second compose service and serve the flow on the monthly schedule from `config.yaml` (`0 6 15 * *`, Europe/Berlin).
