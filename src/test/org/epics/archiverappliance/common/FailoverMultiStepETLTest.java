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
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

import static org.epics.archiverappliance.TomcatSetup.BUILD_TOMCATS_TOMCAT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_NAME;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_PORT;
import static org.epics.archiverappliance.TomcatSetup.TOMCAT_LTS;
import static org.epics.archiverappliance.TomcatSetup.TOMCAT_MTS;
import static org.epics.archiverappliance.TomcatSetup.TOMCAT_STS;
import static org.epics.archiverappliance.common.FailoverTestUtil.generateData;
import static org.epics.archiverappliance.common.FailoverTestUtil.updateLocalPVTypeInfo;
import static org.epics.archiverappliance.common.FailoverTestUtil.updatePVTypeInfo;

/**
 * A more complex test for testing ETL for failover.
 * "Other" generates even data for multiple months
 * "dest" generates odd data for multiple months.
 * We run ETL multiple times...
 * @author mshankar
 *
 */
@Tag("integration")
class FailoverMultiStepETLTest {
    private static final Logger logger = LogManager.getLogger(FailoverMultiStepETLTest.class.getName());
    String pvName = "FailoverETLTest";
    TomcatSetup tomcatSetup = new TomcatSetup();
    long stepSeconds = 3600;
    private ConfigServiceForTests configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", TOMCAT_STS);
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", TOMCAT_MTS);
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", TOMCAT_LTS);
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    /**
     * Generate a months of data for the other appliance.
     *
     * @param startTime     - The query start time
     * @param endTime       - The query end time
     * @param genEventCount - The expected event count.
     * @throws Exception
     */
    private long registerPVForOther(Instant startTime, Instant endTime, long genEventCount) throws Exception {
        updatePVTypeInfo(
                "http://localhost:" + FAILOVER_DEST_PORT, ConfigServiceForTests.TESTAPPLIANCE0, configService, pvName);

        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + FAILOVER_DEST_PORT + "/retrieval/data/getData.raw");
        long rtvlEventCount = 0;
        try (EventStream stream = rawDataRetrieval.getDataForPVS(new String[] {pvName}, startTime, endTime, null)) {
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

    private void changeMTSForDest() throws Exception {
        String otherURL = "pbraw://localhost?name=MTS&rawURL="
                + URLEncoder.encode(
                        "http://localhost:" + FAILOVER_DEST_PORT + "/retrieval/data/getData.raw",
                        StandardCharsets.UTF_8);
        updateLocalPVTypeInfo(configService.getMyApplianceInfo().getIdentity(), configService, pvName, otherURL);
    }

    private long testMergedRetrieval(Instant startTime, Instant endTime) throws Exception {
        long rtvlEventCount = 0;
        long lastEvEpoch = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=" + BUILD_TOMCATS_TOMCAT
                        + this.getClass().getSimpleName() + "/" + FAILOVER_DEST_NAME + "/lts"
                        + "&partitionGranularity=PARTITION_YEAR",
                configService);
        try (BasicContext context = new BasicContext()) {
            logger.info("Looking for data " + plugin.getDescription() + " from "
                    + TimeUtils.convertToHumanReadableString(startTime) + " and "
                    + TimeUtils.convertToHumanReadableString(endTime));
            List<Callable<EventStream>> callables =
                    plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
            for (Callable<EventStream> callable : callables) {
                EventStream ev = callable.call();
                for (Event e : ev) {
                    long evEpoch = TimeUtils.convertToEpochSeconds(e.getEventTimeStamp());
                    logger.debug("Current event " + TimeUtils.convertToHumanReadableString(evEpoch) + " Previous: "
                            + TimeUtils.convertToHumanReadableString(lastEvEpoch));
                    if (lastEvEpoch != 0) {
                        Assertions.assertTrue(
                                evEpoch > lastEvEpoch,
                                "We got events out of order " + TimeUtils.convertToHumanReadableString(lastEvEpoch)
                                        + " and  " + TimeUtils.convertToHumanReadableString(evEpoch)
                                        + " at event count " + rtvlEventCount);
                    }
                    lastEvEpoch = evEpoch;
                    rtvlEventCount++;
                }
            }
        }
        return rtvlEventCount;
    }

    @Test
    void testETL() throws Exception {
        configService.getETLLookup().manualControlForUnitTests();
        // Register the PV with both appliances and generate data.
        Instant startTime = TimeUtils.minusDays(TimeUtils.now(), 365);
        Instant endTime = TimeUtils.now();

        long oCount = 0;
        for (Instant ts = startTime; ts.isBefore(endTime); ts = TimeUtils.plusDays(ts, 1)) {
            oCount = oCount
                    + generateData(
                            ConfigServiceForTests.TESTAPPLIANCE0,
                            ts,
                            0,
                            configService,
                            pvName,
                            stepSeconds,
                            this.getClass().getSimpleName(),
                            "MTS",
                            PartitionGranularity.PARTITION_DAY);
        }
        registerPVForOther(
                TimeUtils.minusDays(TimeUtils.now(), 5 * 365), TimeUtils.plusDays(TimeUtils.now(), 10), oCount);

        System.getProperties()
                .put(
                        "ARCHAPPL_SHORT_TERM_FOLDER",
                        BUILD_TOMCATS_TOMCAT + this.getClass().getSimpleName() + "/" + FAILOVER_DEST_NAME + "/sts");
        System.getProperties()
                .put(
                        "ARCHAPPL_MEDIUM_TERM_FOLDER",
                        BUILD_TOMCATS_TOMCAT + this.getClass().getSimpleName() + "/" + FAILOVER_DEST_NAME + "/mts");
        System.getProperties()
                .put(
                        "ARCHAPPL_LONG_TERM_FOLDER",
                        BUILD_TOMCATS_TOMCAT + this.getClass().getSimpleName() + "/" + FAILOVER_DEST_NAME + "/lts");

        long dCount = 0;
        for (Instant ts = startTime; ts.isBefore(endTime); ts = TimeUtils.plusDays(ts, 1)) {
            oCount = oCount
                    + generateData(
                            FAILOVER_DEST_NAME,
                            ts,
                            1,
                            configService,
                            pvName,
                            stepSeconds,
                            this.getClass().getSimpleName(),
                            "MTS",
                            PartitionGranularity.PARTITION_DAY);
        }
        testMergedRetrieval(TimeUtils.minusDays(TimeUtils.now(), 5 * 365), TimeUtils.plusDays(TimeUtils.now(), 10));
        long totCount = dCount + oCount;

        changeMTSForDest();
        long lastCount = 0;
        for (Instant ts = startTime; ts.isBefore(endTime); ts = TimeUtils.plusDays(ts, 1)) {
            Instant queryStart = TimeUtils.minusDays(TimeUtils.now(), 5 * 365), queryEnd = TimeUtils.plusDays(ts, 10);
            Instant timeETLruns = TimeUtils.getNextPartitionFirstSecond(ts, PartitionGranularity.PARTITION_DAY)
                    .plusSeconds(60);
            // Add 3 days to take care of the hold and gather
            timeETLruns = TimeUtils.plusDays(timeETLruns, 3);
            logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
            ETLExecutor.runETLs(configService, timeETLruns);
            long rCount = testMergedRetrieval(queryStart, queryEnd);
            logger.info("Got " + rCount + " events between " + TimeUtils.convertToHumanReadableString(queryStart)
                    + " and " + TimeUtils.convertToHumanReadableString(queryEnd));
            Assertions.assertTrue(
                    (rCount >= lastCount),
                    "We expected more than what we got last time " + lastCount + " . This time we got " + rCount);
            lastCount = rCount;
        }
        Assertions.assertEquals(lastCount, totCount, "We expected event count  " + totCount + " but got  " + lastCount);
    }
}
