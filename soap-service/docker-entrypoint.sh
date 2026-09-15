#!/usr/bin/env bash
# Boot the legacy SOAP service. Seeds the order history store on first boot only
# (HELIOS_AUTO_SEED=false skips this, used by `make test-soap`).
set -euo pipefail

if [ "${HELIOS_AUTO_SEED:-true}" = "true" ]; then
  python -m app.seed --if-empty
fi

exec "$@"
