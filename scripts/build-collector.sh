#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"
if [[ -n "${OCB_BIN:-}" ]]; then
  command -v "${OCB_BIN}" >/dev/null
elif command -v builder >/dev/null 2>&1; then
  OCB_BIN="$(command -v builder)"
elif command -v ocb >/dev/null 2>&1; then
  OCB_BIN="$(command -v ocb)"
else
  echo 'Install the Collector Builder: go install go.opentelemetry.io/collector/cmd/builder@v0.161.0' >&2
  exit 1
fi
go test -mod=readonly ./...
rm -rf "${ROOT_DIR}/_build"
"${OCB_BIN}" --config examples/otelcol/builder-config.yaml
echo 'Built ./_build/otelcol-jevtraces'
