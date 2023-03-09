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
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.epics.archiverappliance.TomcatSetup.BUILD_TOMCATS_TOMCAT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_NAME;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_PORT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_OTHER_NAME;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_OTHER_PORT;
import static org.epics.archiverappliance.common.FailoverTestUtil.changeMTSForDest;
import static org.epics.archiverappliance.common.FailoverTestUtil.updatePVTypeInfo;

/**
 * Test the getDataAtTime API when using the merge dedup plugin.
 * Generate data such that each failover cluster has one of known data on the morning or afternoon.
 * @author mshankar
 *
 */
@Tag("integration")
public class FailoverScoreAPITest {
    private static final Logger logger = LogManager.getLogger(FailoverScoreAPITest.class.getName());
    String pvName = "FailoverRetrievalTest";
    TomcatSetup tomcatSetup = new TomcatSetup();
    private ConfigServiceForTests configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        tomcatSetup.setUpFailoverWithWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    /**
     * Generate a months worth of data for the given appserver; one per day, a boolean indicating if the sample is in
     * the morning or afternoon.
     *
     * @param applURL       - The URL for the appliance.
     * @param applianceName - The name of the appliance
     * @param theMonth      - The month we generate data for. We generate a month's worth of MTS data.
     * @param morningp      - If true; data is generated for the morning else afternoon.
     * @throws Exception
     */
    private void generateMTSData(String applURL, String applianceName, Instant theMonth, boolean morningp)
            throws Exception {
        int genEventCount = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=MTS&rootFolder=" + BUILD_TOMCATS_TOMCAT
                        + this.getClass().getSimpleName() + "/" + applianceName + "/mts"
                        + "&partitionGranularity=PARTITION_DAY",
                configService);
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream strm = new ArrayListEventStream(
                    0,
                    new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, TimeUtils.getYear(theMonth)));
            for (Instant s = TimeUtils.getPreviousPartitionLastSecond(theMonth, PartitionGranularity.PARTITION_DAY)
                            .plusSeconds(1);
                    s.isBefore(TimeUtils.now());
                    s = s.plusSeconds(PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk())) {

                POJOEvent pojoEvent = new POJOEvent(
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        s.plusSeconds(
                                morningp
                                        ? 10L * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()
                                        : 20L * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()),
                        new ScalarValue<Double>((double) (morningp ? 10 : 20)),
                        0,
                        0);
                logger.debug(
                        "Generating event at " + TimeUtils.convertToHumanReadableString(pojoEvent.getEventTimeStamp()));
                strm.add(pojoEvent);
                genEventCount++;
            }
            plugin.appendData(context, pvName, strm);
        }
        logger.info("Done generating data for appliance " + applianceName);
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
                                PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                                "We got events more than "
                                        + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk()
                                        + " seconds apart " + TimeUtils.convertToHumanReadableString(lastEvEpoch)
                                        + " and  " + TimeUtils.convertToHumanReadableString(evEpoch));
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
    }

    @SuppressWarnings("unchecked")
    private void testDataAtTime(long epochSecs, boolean morningp) throws Exception {
        String scoreURL = "http://localhost:" + FAILOVER_DEST_PORT + "/retrieval/data/getDataAtTime.json?at="
                + TimeUtils.convertToISO8601String(epochSecs);
        JSONArray array = new JSONArray();
        array.add(pvName);
        Map<String, Map<String, Object>> ret =
                (Map<String, Map<String, Object>>) GetUrlContent.postDataAndGetContentAsJSONObject(scoreURL, array);
        Assertions.assertFalse(ret.isEmpty(), "We expected some data back from getDataAtTime");
        for (String retpvName : ret.keySet()) {
            Map<String, Object> val = ret.get(retpvName);
            if (retpvName.equals(pvName)) {
                logger.info("Asking for value at " + TimeUtils.convertToISO8601String(epochSecs) + " got value at "
                        + TimeUtils.convertToISO8601String((long) val.get("secs")));
                Assertions.assertEquals(
                        (double) val.get("val"),
                        (morningp ? 10 : 20),
                        "We expected a morning value for " + TimeUtils.convertToISO8601String(epochSecs)
                                + " instead we got " + TimeUtils.convertToISO8601String((long) val.get("secs")));
                return;
            }
        }

        Assertions.fail("We did not receive a value for PV ");
    }

    @Test
    public void testRetrieval() throws Exception {
        // Register the PV with both appliances and generate data.
        Instant lastMonth = TimeUtils.startOfPreviousMonth(TimeUtils.now());
        generateMTSData("http://localhost:" + FAILOVER_DEST_PORT, FAILOVER_DEST_NAME, lastMonth, true);
        generateMTSData("http://localhost:" + FAILOVER_OTHER_PORT, FAILOVER_OTHER_NAME, lastMonth, false);

        changeMTSForDest(pvName);

        for (int i = 0; i < 20; i++) {
            long startOfDay = (TimeUtils.convertToEpochSeconds(TimeUtils.minusDays(TimeUtils.now(), -1 * (i - 25)))
                            / PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk())
                    * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk();
            for (int h = 0; h < 24; h++) {
                logger.info("Looking for value of PV at  "
                        + TimeUtils.convertToHumanReadableString(startOfDay
                                + (long) h * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk()));
                testDataAtTime(
                        startOfDay + (long) h * PartitionGranularity.PARTITION_HOUR.getApproxSecondsPerChunk(),
                        (h >= 10 && h < 20));
            }
        }
    }
}
