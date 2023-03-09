package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.PVStatus;
import org.epics.archiverappliance.common.TimeUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.epics.archiverappliance.ArchiverTestClient.ArchiveEndPoint;
import static org.epics.archiverappliance.ArchiverTestClient.aliasPV;
import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.archivePVs;
import static org.epics.archiverappliance.ArchiverTestClient.changeParams;
import static org.epics.archiverappliance.ArchiverTestClient.cleanUpPVs;
import static org.epics.archiverappliance.ArchiverTestClient.deletePVs;
import static org.epics.archiverappliance.ArchiverTestClient.getCurrentPVStatus;
import static org.epics.archiverappliance.ArchiverTestClient.getCurrentStatus;
import static org.epics.archiverappliance.ArchiverTestClient.pausePVs;
import static org.epics.archiverappliance.ArchiverTestClient.removeAliasPV;
import static org.epics.archiverappliance.ArchiverTestClient.requestAction;
import static org.epics.archiverappliance.ArchiverTestClient.retrievalCount;

/**
 * Use the firefox driver to test operator's adding a PV to the system.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
class MgmtWorkflowsTest {
    public static final String SAMPLINGPERIOD = "samplingPeriod";
    public static final String HIHI_FIELD = ".HIHI";
    private static final Logger logger = LogManager.getLogger(MgmtWorkflowsTest.class.getName());
    private static final String pvPrefix = MgmtWorkflowsTest.class.getSimpleName();
    static TomcatSetup tomcatSetup = new TomcatSetup();
    static SIOCSetup siocSetup = new SIOCSetup(pvPrefix);
    List<String> pvs = Stream.of(
                    "UnitTestNoNamingConvention:sine",
                    "UnitTestNoNamingConvention:cosine",
                    "test_0",
                    "test_1",
                    "test_2")
            .map(pv -> pvPrefix + pv)
            .toList();

    @BeforeAll
    public static void setUp() throws Exception {
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(MgmtWorkflowsTest.class.getSimpleName());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @AfterEach
    public void cleanUp() throws Exception {
        cleanUpPVs(pvs);
    }

    @Test
    void testSimpleArchivePV() throws Exception {

        archivePV(pvs.get(0));

        List<PVStatus> statuses = getCurrentStatus(List.of(pvs.get(0)));
        Assertions.assertEquals(statuses, List.of(PVStatus.BEING_ARCHIVED));
    }

    @Test
    void testDeleteMultiplePV() throws Exception {

        logger.info("Archiving 5 PV");
        archivePVs(pvs);

        logger.info("Pausing 2 PV");
        List<String> pausingPVs = List.of(pvs.get(2), pvs.get(3));
        pausePVs(pausingPVs);
        Thread.sleep(2 * 1000);

        logger.info("Deleting 2 PV");
        deletePVs(pausingPVs);

        logger.info("Checking other PV are still there");
        List<PVStatus> statuses = getCurrentStatus(List.of(pvs.get(0), pvs.get(1), pvs.get(4)));
        Assertions.assertEquals(
                statuses, List.of(PVStatus.BEING_ARCHIVED, PVStatus.BEING_ARCHIVED, PVStatus.BEING_ARCHIVED));
    }

    @Test
    void testAddRemoveAlias() throws Exception {
        String pvNameToArchive = pvs.get(0);
        String pvNameAlias = pvPrefix + "UnitTestNoNamingConvention:arandomalias";
        String changedPV = pvNameToArchive + HIHI_FIELD;
        String changedPVAliased = pvNameAlias + HIHI_FIELD;

        archivePV(pvNameToArchive);

        SIOCSetup.caput(changedPV, 2.0);
        logger.info("Done updating " + changedPV);
        Thread.sleep(20 * 1000);

        // Test retrieval of data using the real name and the aliased name
        testRetrievalCount(pvNameToArchive, true);
        testRetrievalCount(pvNameAlias, false);
        testRetrievalCount(changedPV, true);
        testRetrievalCount(changedPVAliased, false);

        aliasPV(pvNameToArchive, pvNameAlias);

        testRetrievalCount(pvNameToArchive, true);
        testRetrievalCount(pvNameAlias, true);
        testRetrievalCount(changedPV, true);
        testRetrievalCount(changedPVAliased, true);

        removeAliasPV(pvNameToArchive, pvNameAlias);

        testRetrievalCount(pvNameToArchive, true);
        testRetrievalCount(pvNameAlias, false);
        testRetrievalCount(changedPV, true);
        testRetrievalCount(changedPVAliased, false);
    }

    @Test
    void testArchiveAliasedPV() throws Exception {
        String pvNameToArchive = pvs.get(0);
        String pvNameAlias = pvNameToArchive + "alias";
        archivePV(pvNameToArchive);
        String changedPV = pvNameToArchive + HIHI_FIELD;
        SIOCSetup.caput(changedPV, 2.0);
        Thread.sleep(2 * 1000);
        SIOCSetup.caput(changedPV, 3.0);
        Thread.sleep(2 * 1000);
        SIOCSetup.caput(changedPV, 4.0);
        Thread.sleep(2 * 1000);
        logger.info("Done updating " + changedPV);
        Thread.sleep(60 * 1000);

        List<PVStatus> statuses = getCurrentStatus(List.of(pvNameAlias));
        Assertions.assertEquals(statuses, List.of(PVStatus.BEING_ARCHIVED));

        // Test retrieval of data using the real name and the aliased name
        testRetrievalCount(pvNameToArchive, true);
        testRetrievalCount(pvNameAlias, true);
        testRetrievalCount(changedPV, true);
        testRetrievalCount(pvNameAlias + HIHI_FIELD, true);
    }

    @Test
    void testArchiveFieldsPV() throws Exception {
        String basePV = pvs.get(0);
        String nonExistantPv = basePV + ".YYZ";
        List<String> pvs = Stream.of(
                        basePV, basePV + ".EOFF", basePV + ".EGU", basePV + ".ALST", basePV + ".HOPR", basePV + ".DESC")
                .toList();

        requestAction(List.of(nonExistantPv), ArchiveEndPoint, PVStatus.INITIAL_SAMPLING);
        archivePVs(pvs);

        List<PVStatus> statuses = getCurrentStatus(pvs);
        Assertions.assertEquals(statuses, Collections.nCopies(pvs.size(), PVStatus.BEING_ARCHIVED));

        List<PVStatus> nonStatues = getCurrentStatus(List.of(nonExistantPv));
        Assertions.assertEquals(nonStatues, List.of(PVStatus.INITIAL_SAMPLING));
    }

    @Test
    void testArchiveWithSamplingPeriod() {
        String pvNameToArchive = pvs.get(0);
        try {
            archivePVs(List.of(pvNameToArchive), "SCAN", "3.4");
        } catch (IOException e) {
            Assertions.fail(e);
        }
        JSONArray jsonArray = null;
        try {
            jsonArray = getCurrentPVStatus(List.of(pvNameToArchive));
        } catch (IOException e) {
            Assertions.fail(e);
        }
        JSONObject jsonObject = (JSONObject) jsonArray.get(0);
        Assertions.assertEquals(PVStatus.BEING_ARCHIVED.toString(), jsonObject.get("status"));
        Assertions.assertEquals("3.4", jsonObject.get(SAMPLINGPERIOD));
    }

    @Test
    void testChangeArchivalParams() {
        String pvNameToArchive = pvs.get(0);

        try {
            archivePVs(List.of(pvNameToArchive));
        } catch (IOException e) {
            Assertions.fail(e);
        }
        JSONArray jsonArray = null;
        try {
            jsonArray = getCurrentPVStatus(List.of(pvNameToArchive));
        } catch (IOException e) {
            Assertions.fail(e);
        }
        JSONObject jsonObject = (JSONObject) jsonArray.get(0);
        Assertions.assertEquals(PVStatus.BEING_ARCHIVED.toString(), jsonObject.get("status"));
        Assertions.assertEquals("1.0", jsonObject.get(SAMPLINGPERIOD));

        changeParams(pvNameToArchive, "3.4", "MONITOR");

        try {
            jsonArray = getCurrentPVStatus(List.of(pvNameToArchive));
        } catch (IOException e) {
            Assertions.fail(e);
        }
        jsonObject = (JSONObject) jsonArray.get(0);
        Assertions.assertEquals(PVStatus.BEING_ARCHIVED.toString(), jsonObject.get("status"));
        Assertions.assertEquals("3.4", jsonObject.get(SAMPLINGPERIOD));
    }

    @Test
    void testVALNoVALTest() throws Exception {

        String pvNameToArchive1 = pvs.get(0);
        String pvNameToArchive2 = pvs.get(1);

        String valField = ".VAL";
        archivePVs(List.of(pvNameToArchive1, pvNameToArchive2 + valField));

        testRetrievalCount(pvNameToArchive1, true, 10);
        testRetrievalCount(pvNameToArchive1 + valField, true, 10);
        testRetrievalCount(pvNameToArchive2, true, 10);
        testRetrievalCount(pvNameToArchive2 + valField, true, 10);
    }

    /**
     * Make sure we get some data when retriving under the given name
     * @param pvName
     * @param expectingData - true if we are expecting any data at all.
     * @throws IOException
     */
    private void testRetrievalCount(String pvName, boolean expectingData, long minEvents) throws IOException {
        Instant end = TimeUtils.plusDays(TimeUtils.now(), 3);
        Instant start = TimeUtils.minusDays(end, 6);
        long eventCount = retrievalCount(pvName, start, end);

        logger.info("Got " + eventCount + " event for pv " + pvName);
        if (expectingData) {
            Assertions.assertTrue(
                    eventCount > minEvents,
                    "When asking for data using " + pvName + ", event count is " + minEvents + ". We got "
                            + eventCount);
        } else {
            Assertions.assertEquals(
                    0, eventCount, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
        }
    }
    /**
     * Make sure we get some data when retriving under the given name
     * @param pvName
     * @param expectingData - true if we are expecting any data at all.
     * @throws IOException
     */
    private void testRetrievalCount(String pvName, boolean expectingData) throws IOException {
        testRetrievalCount(pvName, expectingData, 0);
    }
}
