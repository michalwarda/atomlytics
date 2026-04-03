#!/usr/bin/env bash
set -euo pipefail

cd /workspace

cleanup() {
  if [[ -n "${TAILWIND_PID:-}" ]]; then
    kill "${TAILWIND_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

pnpm install --frozen-lockfile
pnpm build:css
pnpm watch:css &
TAILWIND_PID=$!

exec cargo watch \
  -w Cargo.toml \
  -w app/Cargo.toml \
  -w migration/Cargo.toml \
  -w app/src \
  -w migration/src \
  -w regexes.yaml \
  -x "run -p atomlytics"
