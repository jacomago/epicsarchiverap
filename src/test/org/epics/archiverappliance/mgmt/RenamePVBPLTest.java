package org.epics.archiverappliance.mgmt;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.EpicsMessage;
import org.epics.archiverappliance.retrieval.client.GenMsgIterator;
import org.epics.archiverappliance.retrieval.client.RawDataRetrieval;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.deletePVs;
import static org.epics.archiverappliance.ArchiverTestClient.pausePV;
import static org.epics.archiverappliance.retrieval.client.RetreivalTestUtil.mergeHeaders;

/**
 * Test rename PV with data at the backend.
 * We create data in the LTS and then pause, rename and check to make sure we have the same number of samples before and after.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
class RenamePVBPLTest {
    private static final Logger logger = LogManager.getLogger(RenamePVBPLTest.class.getName());
    private static final String pvPrefix = RenamePVBPLTest.class.getSimpleName();
    private final String pvName = pvPrefix + "UnitTestNoNamingConvention:inactive1";
    private final short currentYear = TimeUtils.getCurrentYear();
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup(pvPrefix);
    StoragePlugin storageplugin;

    @BeforeEach
    public void setUp() throws Exception {
        ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        storageplugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        try (BasicContext context = new BasicContext()) {
            // Create three years worth of data in the LTS
            for (short y = 3; y >= 0; y--) {
                short year = (short) (currentYear - y);
                ArrayListEventStream testData = new ArrayListEventStream(
                        24 * 60 * 60,
                        new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));

                for (int day = 0; day < 366; day++) {
                    int startofdayinseconds = day * 24 * 60 * 60;
                    // The value should be the secondsIntoYear integer divided by 600.
                    testData.add(new SimulationEvent(
                            startofdayinseconds, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)
                                    (((int) (startofdayinseconds) / 600)))));
                }
                storageplugin.appendData(context, pvName, testData);
            }
        }
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
        // We have now archived this PV, get some data and validate we got the expected number of events
        long beforeRenameCount = checkRetrieval(pvName);
        logger.info("Before renaming, we had this many events from retrieval" + beforeRenameCount);

        // Let's pause the PV.
        String pausePVURL = "http://localhost:17665/mgmt/bpl/pauseArchivingPV?pv="
                + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject pauseStatus = GetUrlContent.getURLContentAsJSONObject(pausePVURL);
        Assertions.assertTrue(
                pauseStatus.containsKey("status") && pauseStatus.get("status").equals("ok"), "Cannot pause PV");
        logger.info("Successfully paused the PV " + pvName);

        // Let's rename the PV.
        String newPVName = "NewName_" + pvName;
        String renamePVURL =
                "http://localhost:17665/mgmt/bpl/renamePV?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&newname=" + URLEncoder.encode(newPVName, StandardCharsets.UTF_8);
        JSONObject renameStatus = GetUrlContent.getURLContentAsJSONObject(renamePVURL);
        Assertions.assertTrue(
                renameStatus.containsKey("status") && renameStatus.get("status").equals("ok"), "Cannot rename PV");

        long afterRenameCount = checkRetrieval(newPVName);
        logger.info("After renaming, we had this many events from retrieval" + beforeRenameCount);
        // The  Math.abs(beforeRenameCount-afterRenameCount) < 2 is to cater to the engine not sending data after rename
        // as the PV is still paused.
        Assertions.assertTrue(
                Math.abs(beforeRenameCount - afterRenameCount) < 2,
                "Different event counts before and after renaming. Before " + beforeRenameCount + " and after "
                        + afterRenameCount);

        // Make sure the old PV still exists
        long afterRenameOldPVCount = checkRetrieval(pvName);
        Assertions.assertTrue(
                Math.abs(beforeRenameCount - afterRenameOldPVCount) < 2,
                "After the rename, we were still expecting data for the old PV " + afterRenameOldPVCount);

        // Delete the old PV
        String deletePVURL = "http://localhost:17665/mgmt/bpl/deletePV?pv="
                + URLEncoder.encode(pvName, StandardCharsets.UTF_8) + "&deleteData=true";
        JSONObject deletePVtatus = GetUrlContent.getURLContentAsJSONObject(deletePVURL);
        Assertions.assertTrue(
                deletePVtatus.containsKey("status")
                        && deletePVtatus.get("status").equals("ok"),
                "Cannot delete old PV");
        logger.info("Done with deleting the old PV....." + pvName);
        Thread.sleep(3000);

        // Let's rename the PV back to its original name
        String renamePVBackURL =
                "http://localhost:17665/mgmt/bpl/renamePV?pv=" + URLEncoder.encode(newPVName, StandardCharsets.UTF_8)
                        + "&newname=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        JSONObject renameBackStatus = GetUrlContent.getURLContentAsJSONObject(renamePVBackURL);
        Assertions.assertTrue(
                renameBackStatus.containsKey("status")
                        && renameBackStatus.get("status").equals("ok"),
                "Cannot rename PV");
        Thread.sleep(5000);

        long afterRenamingBackCount = checkRetrieval(pvName);
        logger.info("After renaming back to original, we had this many events from retrieval" + afterRenamingBackCount);
        Assertions.assertTrue(
                Math.abs(beforeRenameCount - afterRenamingBackCount) < 2,
                "Different event counts before and after renaming back. Before " + beforeRenameCount + " and after "
                        + afterRenamingBackCount);
    }

    private int checkRetrieval(String retrievalPVName) throws IOException {
        long startTimeMillis = System.currentTimeMillis();
        RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant now = TimeUtils.now();
        Instant start = TimeUtils.minusDays(now, 3 * 366);
        int eventCount = 0;

        final HashMap<String, String> metaFields = new HashMap<String, String>();
        // Make sure we get the EGU as part of a regular VAL call.
        try (GenMsgIterator strm = rawDataRetrieval.getDataForPV(
                retrievalPVName, TimeUtils.toSQLTimeStamp(start), TimeUtils.toSQLTimeStamp(now), false, null)) {
            PayloadInfo info = null;
            Assertions.assertNotNull(strm, "We should get some data, we are getting a null stream back");
            info = strm.getPayLoadInfo();
            Assertions.assertNotNull(info, "Stream has no payload info");
            mergeHeaders(info, metaFields);
            strm.onInfoChange(info1 -> mergeHeaders(info1, metaFields));

            long endTimeMillis = System.currentTimeMillis();

            for (@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
                eventCount++;
            }

            logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
        }

        Assertions.assertTrue(eventCount >= 3 * 366, "Expecting " + 3 * 366 + "events. We got " + eventCount);
        return eventCount;
    }
}
