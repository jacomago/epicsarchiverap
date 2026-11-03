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
four WARs via Gretty's farm. Set production runtime environment variables first;
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

Use `bash appliance.sh start` for background mode and `bash appliance.sh stop`
for graceful shutdown (use absolute script paths from other working directories).
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

This setup preserves independent WAR classloaders and uses the application's
normal startup coordination; it does not rely on the test harness's shared
Log4j readiness appender.

## Smoke test

```sh
python3 src/standalone/smoke_test.py build/output/appliance-<version>.zip
```

Alternatively, build and test in one command:

```sh
ARCHAPPL_SITEID=default ./gradlew standaloneSmokeTest
```

The Python-standard-library test unpacks the real ZIP outside the repository,
creates isolated JDBM2 configuration, checks missing configuration rejection,
starts all four contexts, polls startup state, stops and restarts, and checks that
persistence files survive. It retains its temporary workspace/logs for diagnosis.
Run it on a machine with enough memory for four WAR classloaders and no other
appliance using conflicting auxiliary services. No IOC is needed for this test;
it does not establish archive/retrieve correctness or durable PV metadata recovery.
Use the existing IOC-backed suites for those checks. Port-conflict failure behavior
also requires further validation before production rollout.

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
