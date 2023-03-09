package org.epics.archiverappliance.retrieval.extrafields;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.deletePVs;
import static org.epics.archiverappliance.ArchiverTestClient.pausePV;
import static org.epics.archiverappliance.ArchiverTestClient.resumePV;
import static org.epics.archiverappliance.retrieval.client.RetreivalTestUtil.mergeHeaders;

/**
 * We want to make sure we capture changes in EGU and return them as part of the retrieval request.
 * We archive a PV, then get data (and thus the EGU).
 * We then caput the EGU and then fetch the data again...
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
class EGUChangeTest {
    private final String pvPrefix = EGUChangeTest.class.getSimpleName();
    private final String pvName = pvPrefix + "UnitTestNoNamingConvention:sine";
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup(pvPrefix);

    @BeforeEach
    public void setUp() throws Exception {
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        pausePV(pvName);
        deletePVs(List.of(pvName), true);
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    void testSimpleArchivePV() throws Exception {
        archivePV(pvName);
        // We have now archived this PV, get some data and make sure the EGU is as expected.
        checkEGU("apples");
        SIOCSetup.caput(pvName + ".EGU", "oranges");
        Thread.sleep(5 * 1000);
        // Pause and resume the PV to reget the meta data
        pausePV(pvName);
        resumePV(pvName);

        // Now check the EGU again...
        Thread.sleep(10 * 1000);
        checkEGU("oranges");
    }

    private void checkEGU(String expectedEGUValue) throws IOException {
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, 100);
        Instant end = TimeUtils.plusDays(now, 10);
        int eventCount = 0;

        final HashMap<String, String> metaFields = new HashMap<String, String>();
        // Make sure we get the EGU as part of a regular VAL call.
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPV(
                pvName, TimeUtils.toSQLTimeStamp(start), TimeUtils.toSQLTimeStamp(end), false, null)) {
            PayloadInfo info = null;
            Assertions.assertNotNull(strm, "We should get some data, we are getting a null stream back");
            info = strm.getPayLoadInfo();
            Assertions.assertNotNull(info, "Stream has no payload info");
            mergeHeaders(info, metaFields);
            strm.onInfoChange(info1 -> mergeHeaders(info1, metaFields));

            for (@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
                eventCount++;
            }
        }

        Assertions.assertTrue(
                eventCount > 0, "We should have gotten some data back in retrieval. We got " + eventCount);
        Assertions.assertEquals(
                expectedEGUValue,
                metaFields.get("EGU"),
                "The final value of EGU is " + metaFields.get("EGU") + ". We expected " + expectedEGUValue);
    }
}
