#!/usr/bin/env bash
# Production wrapper around Gretty's generated scripts. Never removes runtime/data files.
set -euo pipefail
DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
command="${1:-run}"
if (($#)); then shift; fi
case "$command" in
  run|start|restart)
    : "${ARCHAPPL_APPLIANCES:?Set ARCHAPPL_APPLIANCES to an absolute appliances XML path}"
    : "${ARCHAPPL_MYIDENTITY:?Set ARCHAPPL_MYIDENTITY to an identity in appliances XML}"
    : "${ARCHAPPL_PERSISTENCE_LAYER:?Set an explicit persistent configuration backend}"
    [[ "$ARCHAPPL_APPLIANCES" = /* && -r "$ARCHAPPL_APPLIANCES" ]] || {
      echo 'ARCHAPPL_APPLIANCES must be an absolute, readable file' >&2; exit 1;
    }
    case "$ARCHAPPL_PERSISTENCE_LAYER" in
      *InMemoryPersistence*) echo 'In-memory persistence is not supported by this production wrapper' >&2; exit 1 ;;
    esac
    for name in ARCHAPPL_SHORT_TERM_FOLDER ARCHAPPL_MEDIUM_TERM_FOLDER ARCHAPPL_LONG_TERM_FOLDER; do
      value="${!name:-}"
      [[ "$value" = /* ]] || { echo "$name must be an absolute directory path" >&2; exit 1; }
      mkdir -p -- "$value"
      [[ -w "$value" ]] || { echo "$name must be writable" >&2; exit 1; }
    done
    export ARCHAPPL_ALL_APPS_ON_ONE_JVM=true
    ;;
  stop) ;;
  *) echo "Usage: $0 {run|start|stop|restart} [Gretty options]" >&2; exit 2 ;;
esac
# Gretty keeps control-port and container work files in the distribution.
# Give relative logging paths a stable base, independent of the caller's cwd.
cd "$DIR"
exec bash "$DIR/$command.sh" "$@"
