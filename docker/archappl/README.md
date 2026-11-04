# Local IOCs with Podman on macOS

`compose.yml` uses bridge networking and publishes the management UI at
<http://localhost:8080/mgmt/ui/index.html>. MariaDB and storage use the existing
named volumes.

## Channel Access

The container searches `host.containers.internal` on the default CA port 5064.
Archive a bare PV name, e.g. `Ttest_1000`.

## PVAccess

The older pvAccessCPP server used by `softIocPVA` replies to the UDP response
port embedded in the search request. Podman's Mac-side UDP NAT changes the
source port without changing that embedded port, so replies never reach the
container. The Java PVA proxy supports replying to the translated source port.

1. Start the IOC on the Mac with an explicit loopback PVA interface:

   ```bash
   EPICS_PVAS_INTF_ADDR_LIST=127.0.0.1 \
   softIocPVA -m P=T -d src/resources/test/UnitTestPVs.db
   ```

2. Start the containers:

   ```bash
   podman compose up -d
   ```

3. In another Mac terminal, run the proxy and leave it running:

   ```bash
   bash docker/archappl/run-pva-proxy.sh
   ```

   Requires a local Java runtime compatible with the image's `core-pva` JAR
   (tested with Java 25). The script copies that JAR from the running container
   into a temporary directory. It searches the IOC on 127.0.0.1:5076 and serves
   PVA discovery/data on port 5078. If restarting the proxy immediately, wait
   about a minute for the previous TCP connections to clear; verify its log
   says `Listening on TCP ...:5078`, not a randomly selected fallback port.

4. Archive a PV with the `pva://` prefix in the management UI, e.g.
   `pva://Ttest_2000`. Check status:

   ```bash
   curl --get --data-urlencode 'pv=pva://Ttest_2000' \
     http://localhost:8080/mgmt/bpl/getPVStatus
   ```

   Use the same prefixed name for data retrieval:

   ```bash
   curl --get --data-urlencode 'pv=pva://Ttest_2000' \
     http://localhost:8080/retrieval/data/getData.json
   ```

The container hostname is `localhost` so the Mac-side Java proxy can resolve
its hostname during PVA's CA-style authentication. No separate name server is
used. For IOCs elsewhere, set `PVA_IOC_ADDR_LIST` when launching the proxy.

This is a development workaround, not a production gateway. The proxy's UDP
listener is restricted to loopback, but this library's TCP listener binds all
interfaces. Do not expose it to untrusted networks. The proxy must remain
running for PVA archiving; stopping it does not affect Channel Access.
