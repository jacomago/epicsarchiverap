/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_NAME;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_PORT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_OTHER_NAME;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_OTHER_PORT;
import static org.epics.archiverappliance.common.FailoverTestUtil.generateData;
import static org.epics.archiverappliance.common.FailoverTestUtil.updatePVTypeInfo;

/**
 * Test merging in data from an external store.
 * @author mshankar
 *
 */
@Tag("integration")
public class MergeDataFromExternalStoreTest {
    private static final Logger logger = LogManager.getLogger(MergeDataFromExternalStoreTest.class.getName());
    private ConfigServiceForTests configService;
    String pvName = "MergeDataFromExternalStoreTest";
    TomcatSetup tomcatSetup = new TomcatSetup();
    long tCount = 0;
    long stepSeconds = 2;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        tomcatSetup.setUpFailoverWithWebApps(this.getClass().getSimpleName());
    }

    /**
     * Generate a months worth of data for the given appserver.
     * @param applURL - The URL for the appliance.
     * @param applianceName - The name of the appliance
     * @param lastMonth - The month we generate data for. We generate a month's worth of MTS data.
     * @param startingOffset - Use 0 for even seconds; 1 for odd seconds. When merged, we test to make sure; we get data one second apart.
     * @throws Exception
     */
    private long generateMTSData(String applURL, String applianceName, Instant lastMonth, int startingOffset)
            throws Exception {
        int genEventCount = generateData(
                applianceName,
                lastMonth,
                startingOffset,
                configService,
                pvName,
                stepSeconds,
                this.getClass().getSimpleName(),
                "MTS");
        logger.info("Done generating dest data");
        updatePVTypeInfo(applURL, applianceName, configService, pvName);

        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(applURL + "/retrieval/data/getData.raw");
        long rtvlEventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(
                new String[] {pvName},
                TimeUtils.minusDays(TimeUtils.now(), 90),
                TimeUtils.plusDays(TimeUtils.now(), 31),
                null)) {
            long lastEvEpoch = 0;
            if (stream != null) {
                for (Event e : stream) {
                    long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
                    if (lastEvEpoch != 0) {
                        Assertions.assertEquals(
                                (evEpoch - lastEvEpoch),
                                stepSeconds,
                                "We got events more than " + stepSeconds + " seconds apart "
                                        + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  "
                                        + TimeUtils.convertToHumanReadableString(evEpoch));
                    }
                    lastEvEpoch = evEpoch;
                    rtvlEventCount++;
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        Assertions.assertEquals(
                genEventCount,
                rtvlEventCount,
                "We expected event count  " + genEventCount + " but got  " + rtvlEventCount);
        return rtvlEventCount;
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    private void mergeInDataFromRemoteServer() throws Exception {
        JSONObject cresp = GetUrlContent.getURLContentAsJSONObject(
                "http://localhost:" + FAILOVER_DEST_PORT + "/mgmt/bpl/consolidateDataForPV"
                        + "?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&storage=LTS");
        Assertions.assertEquals("ok", cresp.get("status"), "Invalid response for consolidate data");
        String otherClientURL = "http://localhost:" + FAILOVER_OTHER_PORT + "/retrieval";
        JSONObject resp = GetUrlContent.getURLContentAsJSONObject(
                "http://localhost:" + FAILOVER_DEST_PORT + "/mgmt/bpl/mergeInData"
                        + "?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&other=" + URLEncoder.encode(otherClientURL, StandardCharsets.UTF_8)
                        + "&storage=LTS"
                        + "&from=" + TimeUtils.convertToISO8601String(TimeUtils.minusDays(TimeUtils.now(), 366 * 2)));
        Assertions.assertEquals("ok", resp.get("status"), "Invalid response for merge data");
        logger.info("Merged data for " + pvName + " into the dest tomcat");
    }

    private void testMergedRetrieval() throws Exception {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + FAILOVER_DEST_PORT + "/retrieval/data/getData.raw");
        long rtvlEventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(
                new String[] {pvName},
                TimeUtils.minusDays(TimeUtils.now(), 90),
                TimeUtils.plusDays(TimeUtils.now(), 31),
                null)) {
            long lastEvEpoch = 0;
            if (stream != null) {
                for (Event e : stream) {
                    long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
                    logger.debug("Current event " + TimeUtils.convertToHumanReadableString(evEpoch) + " Previous: "
                            + TimeUtils.convertToHumanReadableString(lastEvEpoch));
                    if (lastEvEpoch != 0) {
                        Assertions.assertTrue(
                                evEpoch > lastEvEpoch,
                                "We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch)
                                        + " and  " + TimeUtils.convertToHumanReadableString(evEpoch)
                                        + " at event count " + rtvlEventCount);
                        Assertions.assertEquals(
                                1,
                                (evEpoch - lastEvEpoch),
                                "We got events more than a second apart "
                                        + TimeUtils.convertToHumanReadableString(lastEvEpoch) + " and  "
                                        + TimeUtils.convertToHumanReadableString(evEpoch) + " at event count "
                                        + rtvlEventCount);
                    }
                    lastEvEpoch = evEpoch;
                    rtvlEventCount++;
                }
            } else {
                Assertions.fail("Stream is null when retrieving data.");
            }
        }
        Assertions.assertEquals(
                tCount, rtvlEventCount, "We expected event count  " + tCount + " but got  " + rtvlEventCount);
    }

    @Test
    public void testRetrieval() throws Exception {
        // Register the PV with both appliances and generate data.
        Instant lastMonth = TimeUtils.startOfPreviousMonth(TimeUtils.now());
        long dCount = generateMTSData("http://localhost:" + FAILOVER_DEST_PORT, FAILOVER_DEST_NAME, lastMonth, 0);
        long oCount = generateMTSData("http://localhost:" + FAILOVER_OTHER_PORT, FAILOVER_OTHER_NAME, lastMonth, 1);
        tCount = dCount + oCount;

        mergeInDataFromRemoteServer();
        testMergedRetrieval();
    }
}
