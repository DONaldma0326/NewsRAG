#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../src"
uv run uvicorn api.main:app --reload --port "${PORT:-8000}"
