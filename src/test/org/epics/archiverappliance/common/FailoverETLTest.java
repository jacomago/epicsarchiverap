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
 * Test basic failover - test the ETL side of things.
 * @author mshankar
 *
 */
@Tag("integration")
class FailoverETLTest {
    private static final Logger logger = LogManager.getLogger(FailoverETLTest.class.getName());
    String pvName = "FailoverETLTest";
    TomcatSetup tomcatSetup = new TomcatSetup();
    long stepSeconds = 2;
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
     * Generate a months worth of data.
     *
     * @param lastMonth - The month we generate data for. We generate a month's worth of MTS data.
     */
    private long generateDataAndRegisterPV(Instant lastMonth) throws Exception {
        int genEventCount = generateData(
                ConfigServiceForTests.TESTAPPLIANCE0,
                lastMonth,
                0,
                configService,
                pvName,
                stepSeconds,
                this.getClass().getSimpleName(),
                "MTS");

        updatePVTypeInfo(
                "http://localhost:" + FAILOVER_DEST_PORT, ConfigServiceForTests.TESTAPPLIANCE0, configService, pvName);

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
            List<Callable<EventStream>> callables =
                    plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
            Assertions.assertFalse(callables.isEmpty(), "We got zero callables");
            for (Callable<EventStream> callable : callables) {
                EventStream ev = callable.call();
                logger.error("Event Stream " + ev.getDescription());
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
            }
        }
        return rtvlEventCount;
    }

    @Test
    void testETL() throws Exception {
        configService.getETLLookup().manualControlForUnitTests();
        // Register the PV with both appliances and generate data.
        Instant lastMonth = TimeUtils.startOfPreviousMonth(TimeUtils.now());
        long oCount = generateDataAndRegisterPV(lastMonth);

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
        long dCount = generateData(
                FAILOVER_DEST_NAME,
                lastMonth,
                1,
                configService,
                pvName,
                stepSeconds,
                this.getClass().getSimpleName(),
                "MTS");

        long tCount = dCount + oCount;

        changeMTSForDest();
        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365 * 10);
        logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));

        ETLExecutor.runETLs(configService, timeETLruns);

        logger.info("Checking merged data after running ETL");
        long rCount = testMergedRetrieval(
                TimeUtils.minusDays(TimeUtils.now(), 365 * 2), TimeUtils.plusDays(TimeUtils.now(), 365 * 2));
        Assertions.assertEquals(tCount, rCount, "We expected event count  " + tCount + " but got  " + rCount);
    }
}
