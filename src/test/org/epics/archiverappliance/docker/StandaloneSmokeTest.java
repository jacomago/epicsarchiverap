package org.epics.archiverappliance.docker;

import org.awaitility.Awaitility;
import org.epics.archiverappliance.ArchiveTestUtils;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** End-to-end test of the Gretty ZIP with the same MariaDB and IOC as DockerSmokeTest. */
@Testcontainers
@Tag("docker")
@Tag("standaloneDocker")
public class StandaloneSmokeTest {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneSmokeTest.class);
    private static final String PV_NAME = "IOCtest_0";

    @Container
    static ComposeContainer environment =
            new ComposeContainer(new File("docker/docker-compose.standalone-smoketest.yml"))
                    .withLocalCompose(true)
                    .withExposedService(
                            "archappl", 8080,
                            Wait.forLogMessage(".*All components in this appliance have started up.*", 1)
                                    .withStartupTimeout(Duration.ofMinutes(5)))
                    .withExposedService("mariadb", 3306, Wait.forHealthcheck())
                    .withLogConsumer("archappl", new Slf4jLogConsumer(logger).withPrefix("standalone"))
                    .withLogConsumer("example-ioc", new Slf4jLogConsumer(logger).withPrefix("example-ioc"))
                    .withLogConsumer("mariadb", new Slf4jLogConsumer(logger).withPrefix("mariadb"));

    @Test
    void testArchivePVInStandaloneDistribution() throws Exception {
        String base = "http://" + environment.getServiceHost("archappl", 8080)
                + ":" + environment.getServicePort("archappl", 8080);
        String mgmtUrl = base + "/mgmt/bpl/";
        for (String component : new String[] {"mgmt", "engine", "etl", "retrieval"}) {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .until(() -> "STARTUP_COMPLETE".equals(GetUrlContent.getURLContentAsJSONObject(
                                    base + "/" + component + "/bpl/startupState")
                            .get("status")));
        }
        GetUrlContent.getURLContentAsJSONArray(
                mgmtUrl + "archivePV?pv=" + PV_NAME + "&samplingperiod=1&samplingmethod=MONITOR");
        ArchiveTestUtils.waitForStatusChange(PV_NAME, "Being archived", 12, mgmtUrl, 15);

        // Prove this exercised SQL persistence, not just a running but unused MariaDB service.
        String jdbcUrl = "jdbc:mariadb://" + environment.getServiceHost("mariadb", 3306)
                + ":" + environment.getServicePort("mariadb", 3306) + "/archappl";
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            try (var connection = DriverManager.getConnection(jdbcUrl, "archappl", "archappl");
                    var statement = connection.prepareStatement("SELECT COUNT(*) FROM PVTypeInfo WHERE pvName = ?")) {
                statement.setString(1, PV_NAME);
                try (var rows = statement.executeQuery()) {
                    rows.next();
                    assertEquals(1, rows.getInt(1), "Archived PV must be persisted in MariaDB");
                }
            }
        });
    }
}
