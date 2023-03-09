package org.epics.archiverappliance.engine.V4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.deletePVs;
import static org.epics.archiverappliance.ArchiverTestClient.pausePV;

@Tag("integration")
@Tag("localEpics")
class PVAEpicsIntegrationTest {

    private static final Logger logger = LogManager.getLogger(PVAEpicsIntegrationTest.class.getName());
    private static final String pvPrefix =
            PVAEpicsIntegrationTest.class.getSimpleName().substring(0, 10);
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup(pvPrefix);

    @BeforeEach
    public void setUp() throws Exception {

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        siocSetup.startSIOCWithDefaultDB();
    }

    String pvName = pvPrefix + "UnitTestNoNamingConvention:sine";

    @AfterEach
    public void tearDown() throws Exception {
        pausePV(pvName);
        deletePVs(List.of(pvName), true);

        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    void testEpicsIocPVA() throws Exception {

        logger.info("Starting pvAccess test for pv " + pvName);

        Instant firstInstant = Instant.now();

        // Archive PV
        archivePV("pva://" + pvName);

        long samplingPeriodMilliSeconds = 100;

        Thread.sleep(samplingPeriodMilliSeconds);
        double secondsToBuffer = 5.0;

        Thread.sleep(samplingPeriodMilliSeconds);
        // Need to wait for the writer to write all the received data.
        Thread.sleep((long) secondsToBuffer * 1000);
        Instant end = Instant.now();

        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");

        EventStream stream = null;
        Map<Instant, SampleValue> actualValues = new HashMap<>();
        try {
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {pvName},
                    firstInstant,
                    end,
                    desc -> logger.info("Getting data for PV " + desc.getPvName()));

            // Make sure we get the DBR type we expect
            Assertions.assertEquals(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE, stream.getDescription().getArchDBRType());

            // We are making sure that the stream we get back has times in sequential order...
            for (Event e : stream) {
                actualValues.put(e.getEventTimeStamp(), e.getSampleValue());
            }
        } finally {
            if (stream != null)
                try {
                    stream.close();
                } catch (Throwable ignored) {
                }
        }
        logger.info("Data was {}", actualValues);
        Assertions.assertTrue(actualValues.size() > secondsToBuffer);
    }
}
