#!/usr/bin/env bash
# Development-only PVA proxy: run on the Mac, not inside the Podman VM.
set -euo pipefail

cd "$(dirname "$0")/../.."
command -v java >/dev/null || { echo "Java is required on the Mac." >&2; exit 1; }
container=$(podman compose ps -q epicsarchiver)
[[ -n "$container" ]] || { echo "Start the stack with podman compose up -d first." >&2; exit 1; }
jar=$(podman exec "$container" sh -c 'find /usr/local/tomcat/lib -name "core-pva-*.jar" -print -quit')
[[ -n "$jar" ]] || { echo "No Java PVA library found in the archiver image." >&2; exit 1; }

# Use the same library as the archiver; no extra download or build needed.
work=$(mktemp -d)
work=$(cd "$work" && pwd -P)
pid=
cleanup() {
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
podman cp "$container:$jar" "$work/core-pva.jar"

# IOC searches stay native to the Mac. The proxy's newer PVA protocol replies
# to the UDP source port, rather than the port hidden behind Podman's NAT.
# Port 5078 is reserved here for the proxy (UDP discovery and TCP data).
export EPICS_PVA_ADDR_LIST="${PVA_IOC_ADDR_LIST:-127.0.0.1}"
export EPICS_PVA_AUTO_ADDR_LIST=NO
export EPICS_PVA_NAME_SERVERS=""
export EPICS_PVA_BROADCAST_PORT=5076
export EPICS_PVAS_INTF_ADDR_LIST=127.0.0.1
export EPICS_PVAS_BROADCAST_PORT=5078
export EPICS_PVA_SERVER_PORT=5078
export PREFIX=PodmanPVA:
java -cp "$work/core-pva.jar" org.epics.pva.proxy.PVAProxy &
pid=$!
wait "$pid"
