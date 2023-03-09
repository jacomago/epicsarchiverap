package org.epics.archiverappliance.engine.test;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ArchiverTestClient;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;

import static org.epics.archiverappliance.engine.pv.PVMetrics.CONNECTION_LOST_SECONDS_KEY;

/**
 * Start an appserver with persistence; start archiving a PV; then start and restart the SIOC and make sure we get the expected cnxlost headers.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class CnxLostTest {
    private static final Logger logger = LogManager.getLogger(CnxLostTest.class.getName());
    private final File persistenceFolder =
            new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "CnxLostTest");
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    private static ConnectionLossType determineConnectionLossType(EpicsMessage dbrevent) {
        ConnectionLossType retVal = ConnectionLossType.NONE;
        Map<String, String> extraFields = dbrevent.getFieldValues();
        logger.info("field Values are " + extraFields);
        if (extraFields.containsKey(CONNECTION_LOST_SECONDS_KEY)) {
            String connectionLostSecs = extraFields.get(CONNECTION_LOST_SECONDS_KEY);
            if (Long.parseLong(connectionLostSecs) == 0) {
                Assertions.assertTrue(
                        extraFields.containsKey("startup"), "At least for now, we should have a startup field as well");
                retVal = ConnectionLossType.STARTUP_OR_PAUSE_RESUME;
            } else {
                retVal = ConnectionLossType.IOC_RESTART;
            }
        }

        logger.info("Event with value " + dbrevent.getNumberValue().doubleValue() + " is of type " + retVal);
        return retVal;
    }

    @BeforeEach
    public void setUp() throws Exception {
        if (persistenceFolder.exists()) {
            FileUtils.deleteDirectory(persistenceFolder);
        }
        persistenceFolder.mkdirs();
        System.getProperties()
                .put(
                        ConfigService.ARCHAPPL_PERSISTENCE_LAYER,
                        "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
        System.getProperties()
                .put(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");

        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        siocSetup.stopSIOC();
        tomcatSetup.tearDown();

        if (persistenceFolder.exists()) {
            FileUtils.deleteDirectory(persistenceFolder);
        }

        File mtsFolder = new File(
                ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "UnitTestNoNamingConvention");
        if (mtsFolder.exists()) {
            FileUtils.deleteDirectory(mtsFolder);
        }
    }

    @Test
    public void testConnectionLossHeaders() throws Exception {
        String pvName = "UnitTestNoNamingConvention:inactive1";
        String pvURLName = URLEncoder.encode(pvName, StandardCharsets.UTF_8);

        // Archive PV
        ArchiverTestClient.archivePV(pvName);

        // UnitTestNoNamingConvention:inactive1 is SCAN passive without autosave so it should have an invalid timestamp.
        // We caput something to generate a valid timestamp..
        SIOCSetup.caput(pvName, "1.0");
        Thread.sleep(1 * 1000);
        SIOCSetup.caput(pvName, "2.0");
        Thread.sleep(1 * 1000);

        checkRetrieval(pvName, new ExpectedEventType[] {
            new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
            new ExpectedEventType(ConnectionLossType.NONE, 1)
        });

        // Let's pause the PV.
        ArchiverTestClient.pausePV(pvName);

        SIOCSetup.caput(pvName, "3.0"); // We are paused; so we should miss this event
        Thread.sleep(1 * 1000);
        SIOCSetup.caput(pvName, "4.0");
        Thread.sleep(1 * 1000);

        // Resume PV
        ArchiverTestClient.resumePV(pvName);

        checkRetrieval(pvName, new ExpectedEventType[] {
            new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
            new ExpectedEventType(ConnectionLossType.NONE, 1),
            new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1)
        });

        siocSetup.stopSIOC();
        Thread.sleep(10 * 1000);

        siocSetup = new SIOCSetup();
        siocSetup.startSIOCWithDefaultDB();

        SIOCSetup.caput(pvName, "5.0");
        Thread.sleep(1 * 1000);
        SIOCSetup.caput(pvName, "6.0");
        Thread.sleep(10 * 1000);

        checkRetrieval(pvName, new ExpectedEventType[] {
            new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
            new ExpectedEventType(ConnectionLossType.NONE, 1),
            new ExpectedEventType(ConnectionLossType.STARTUP_OR_PAUSE_RESUME, 1),
            new ExpectedEventType(ConnectionLossType.IOC_RESTART, 1),
            new ExpectedEventType(ConnectionLossType.NONE, 1),
        });
    }

    private void checkRetrieval(String retrievalPVName, ExpectedEventType[] expectedEvents) throws IOException {
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, 366);

        LinkedList<EpicsMessage> retrievedData = new LinkedList<EpicsMessage>();
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPV(
                retrievalPVName, TimeUtils.toSQLTimeStamp(start), TimeUtils.toSQLTimeStamp(now), false, null)) {
            int eventCount = 0;
            Assertions.assertNotNull(strm, "We should get some data, we are getting a null stream back");
            for (EpicsMessage dbrevent : strm) {
                logger.info("Adding event with value "
                        + dbrevent.getNumberValue().doubleValue() + " at time "
                        + TimeUtils.convertToHumanReadableString(TimeUtils.fromSQLTimeStamp(dbrevent.getTimestamp())));
                retrievedData.add(dbrevent);
                eventCount++;
            }
            Assertions.assertTrue(eventCount >= 1, "Expecting at least one event. We got " + eventCount);
        }
        int eventIndex = 0;
        for (ExpectedEventType expectedEvent : expectedEvents) {
            for (int i = 0; i < expectedEvent.numberOfEvents; i++) {
                Assertions.assertFalse(
                        retrievedData.isEmpty(),
                        "Ran out of events at " + eventIndex + " processed " + i + " expecting "
                                + (expectedEvent.numberOfEvents - i) + "more");
                EpicsMessage message = retrievedData.poll();
                Assertions.assertSame(
                        expectedEvent.lossType,
                        determineConnectionLossType(message),
                        "Expecting event at " + eventIndex + " to be of type " + expectedEvent.lossType);
                eventIndex++;
            }
        }
    }

    enum ConnectionLossType {
        STARTUP_OR_PAUSE_RESUME,
        IOC_RESTART,
        NONE
    }

    static class ExpectedEventType {
        ConnectionLossType lossType;
        int numberOfEvents;

        public ExpectedEventType(ConnectionLossType lossType, int numberOfEvents) {
            this.lossType = lossType;
            this.numberOfEvents = numberOfEvents;
        }
    }
}
