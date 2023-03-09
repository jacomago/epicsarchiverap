package org.epics.archiverappliance.retrieval.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.retrievalCount;
import static org.epics.archiverappliance.retrieval.client.RetreivalTestUtil.checkRetrieval;

@Tag("integration")
public class RetrievalClientTest {

    private static final Logger logger = LogManager.getLogger(RetrievalClientTest.class.getName());
    private static final String pvPrefix = RetrievalClientTest.class.getSimpleName();
    private static final String pvName = pvPrefix + "UnitTestNoNamingConvention:inactive1";
    static TomcatSetup tomcatSetup = new TomcatSetup();
    static SIOCSetup siocSetup = new SIOCSetup(pvPrefix);
    static long previousEpochSeconds = 0;
    static short currentYear = TimeUtils.getCurrentYear();

    @BeforeAll
    public static void setUp() throws Exception {
        tomcatSetup.setUpWebApps(RetrievalClientTest.class.getSimpleName());
        siocSetup.startSIOCWithDefaultDB();

        generateDataForPV(pvName);
        generateDataForPV(pvName + 2);
    }

    private static void generateDataForPV(String pvName) throws ConfigException, IOException {
        ConfigServiceForTests configService = new ConfigServiceForTests(1);

        StoragePlugin storageplugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream testData = new ArrayListEventStream(
                    24 * 60 * 60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));

            short year = (short) (currentYear - 1);
            for (int day = 0; day < 366; day++) {
                int startofdayinseconds = day * 24 * 60 * 60;
                // The value should be the secondsIntoYear integer divided by 600.
                testData.add(new SimulationEvent(
                        startofdayinseconds, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<>((double)
                                (startofdayinseconds))));
            }

            storageplugin.appendData(context, pvName, testData);
        }
        configService.shutdownNow();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    void testGetDataForSinglePV() throws Exception {
        testGetOneDaysDataForYear(currentYear - 1, 86400);
        testGetOneDaysDataForYear(currentYear - 2, 0);
        testGetOneDaysDataForYear(currentYear, 1);
    }

    private void testGetOneDaysDataForYear(int year, int expectedCount) throws Exception {
        Instant start = TimeUtils.convertFromISO8601String(year + "-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(year + "-02-02T08:00:00.000Z");

        long eventCount = retrievalCount(pvName, start, end);

        Assertions.assertEquals(
                eventCount,
                expectedCount,
                "Event count is not what we expect. We got " + eventCount + " and we expected " + expectedCount
                        + " for year " + year);
    }

    @Test
    void testMean600() throws Exception {
        logger.info("Start test");
        archivePV(pvName);

        logger.info(" We have now archived this PV, get some data and validate we got the expected number of events");
        checkRetrieval(pvName, 366, false, 3);
        checkRetrieval("mean_600(" + pvName + ")", 366 * 24 * 6, false, 3);
    }

    @Test
    void testGetDataForTwoPVs() {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant start = TimeUtils.getStartOfYear(currentYear - 1);
        Instant end = TimeUtils.getEndOfYear(currentYear - 1);
        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(new String[] {pvName, pvName + 2}, start, end, desc -> {
                logger.info("On the client side, switching to processing PV " + desc.getPvName());
                previousEpochSeconds = 0;
            });

            // We are making sure that the stream we get back has times in sequential order...
            if (stream != null) {
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                }
            }
        } finally {
            if (stream != null)
                try {
                    stream.close();
                    stream = null;
                } catch (Throwable ignored) {
                }
        }
    }
}
