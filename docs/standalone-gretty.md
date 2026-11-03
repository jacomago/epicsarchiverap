# Standalone distribution with Gretty

This opt-in distribution runs one appliance (mgmt, engine, ETL and retrieval) in
one embedded Tomcat 11 process. It is a ZIP, **not an executable fat JAR**.
Java 25 is required; external Tomcat and Gradle are not required at runtime.
Existing WAR/release tasks and test launchers are unchanged.

## Build

```sh
ARCHAPPL_SITEID=default ./gradlew standaloneDist
```

Use your production site ID/path instead of `default` to customize WAR resources.
The standalone tasks reject the implicit `tests` site. Build output is
`build/output/appliance-<version>.zip`, with an unpacked copy in
`build/output/appliance/`. The build retains the normal WAR documentation and
site-specific prerequisites, including Python documentation tooling.

For development, `ARCHAPPL_SITEID=default ./gradlew standaloneRun` runs the same
four WARs via Gretty's farm. `appRun` instead runs the default project webapp and
is not the four-component deployment. Development runners explicitly use the
default site's console Log4j configuration. Set production runtime environment variables first;
this is not the in-memory integration-test harness. The farm uses port 8080.

## Configure and run (Linux/macOS)

Unpack into a dedicated writable directory for each appliance. Keep archive data,
configuration persistence and operator-owned configuration **outside** that
installation directory. Never share an installation/work directory between running
processes. Gretty stores container work and control-port files in the installation;
this first version does not support a read-only installation or a separately
relocated container work directory.

Create an appliances XML with your identity and all four component URLs using the
same host/HTTP port (8080 by default), e.g. `/mgmt/bpl`, `/engine/bpl`, `/etl/bpl`
and `/retrieval/bpl`. Also configure `data_retrieval_url` and a unique
`cluster_inetport` as in a normal installation. Advertise addresses reachable by
all clients and appliances, not merely a local bind address.

Example using file-backed configuration persistence:

```sh
export ARCHAPPL_APPLIANCES=/srv/archiver/config/appliances.xml
export ARCHAPPL_MYIDENTITY=appliance0
export ARCHAPPL_SHORT_TERM_FOLDER=/srv/archiver/data/sts
export ARCHAPPL_MEDIUM_TERM_FOLDER=/srv/archiver/data/mts
export ARCHAPPL_LONG_TERM_FOLDER=/srv/archiver/data/lts
export ARCHAPPL_PERSISTENCE_LAYER=org.epics.archiverappliance.config.persistence.JDBM2Persistence
export ARCHAPPL_PERSISTENCE_LAYER_JDBM2FILENAME=/srv/archiver/config/archive.jdbm2
bash /opt/archiver/appliance/appliance.sh run
```

Create the persistence file's parent directory first. Other supported persistent
backends may be configured using their normal settings. The wrapper requires an
explicit backend and rejects `InMemoryPersistence`. It enables
`ARCHAPPL_ALL_APPS_ON_ONE_JVM`, validates required paths, and does not remove data
on startup or shutdown. Use absolute paths for all external configuration,
policies, logs and persistence. Configure XLTS separately if your policies use it.

Use `bash appliance.sh start` for noninteractive mode and `bash appliance.sh stop`
from another terminal for graceful shutdown (use absolute script paths from other
working directories). `start` keeps the starter attached; use a service manager or
shell backgrounding if needed.
Prefer `stop` followed by `start` over `restart` when verifying upgrades.
Gretty also generates Windows launch scripts, but the validated production wrapper
and smoke test currently target Linux/macOS.

### Port, logging and JVM options

Before startup, edit `conf/server.json` in the unpacked distribution:

- `serverConfig.httpPort`: HTTP port; update appliances XML to match.
- `serverConfig.jvmArgs`: array of JVM arguments, e.g. `-Xmx2g` and
  `-Dlog4j.configurationFile=/srv/archiver/config/log4j2.xml`.

Do not put secrets in a publicly distributed ZIP. Configure a unique cluster port
per appliance as well. Generated Gretty scripts use `java` on `PATH`; select Java
25 there. Gretty runs its container in a child JVM, so JVM options must be set on
that runner, not only on the starter process.

For MariaDB or another JNDI backend, supply a Tomcat context XML when packaging:
`-PstandaloneContext=/absolute/path/context.xml`. Naming is enabled in products;
Tomcat JDBC pool and the MariaDB driver are shipped on the container's classpath.
Use your normal datasource credentials/URL and select `MySQLPersistence` at runtime.
The smoke product uses the repository's Docker context XML for its isolated database.

This setup preserves independent WAR classloaders and uses the application's
normal startup coordination; it does not rely on the test harness's shared
Log4j readiness appender.

## Smoke test

```sh
ARCHAPPL_SITEID=default ./gradlew standaloneSmokeTest
```

Requires Docker. The JUnit/Testcontainers test follows `DockerSmokeTest`: Compose
starts MariaDB 11 (initialized with the existing smoke-test SQL schema), the example
IOC, and the packaged Gretty distribution. The test checks all four startup states,
archives `IOCtest_0`, waits for `Being archived`, and verifies its `PVTypeInfo` row
in MariaDB. Service ports are exposed dynamically to avoid fixed host-port conflicts.
Container logs are forwarded through SLF4J to the Gradle test output/report.

The `applianceSmoke` product uses the same WARs and runtime as `appliance`, with the
existing Docker MariaDB JNDI context added. Its ZIP is unpacked in a Java 25 image;
no external Tomcat is installed. Images are built before Testcontainers starts
Compose, matching the existing Docker smoke-test workflow. The test is opt-in and
excluded from ordinary unit tests and the existing `dockerSmokeTest` task.
Restart/PV recovery, retrieval correctness and port-conflict behavior remain separate
validation work.

## Upgrades and limitations

Stop gracefully, back up configuration/persistence, unpack the new ZIP into a new
installation directory, reapply operator-owned server configuration, and start
using the same external data/configuration paths. Do not copy stale container work
files. Back up before any persistence format migration.

Gretty 5.1.0 supports Tomcat 11; this build pins its embedded Tomcat version to the
project version catalog (currently 11.0.12). Tested build tooling: Java 25 and
Gradle 9.8. Gretty emits Gradle deprecation warnings; Gradle 10 compatibility is
not claimed. Deployment is opt-in and should be evaluated before replacing a
production multi-Tomcat installation.
