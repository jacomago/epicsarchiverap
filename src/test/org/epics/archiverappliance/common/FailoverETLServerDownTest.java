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
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

import static org.epics.archiverappliance.TomcatSetup.BUILD_TOMCATS_TOMCAT;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_NAME;
import static org.epics.archiverappliance.TomcatSetup.FAILOVER_DEST_PORT;
import static org.epics.archiverappliance.common.FailoverTestUtil.generateData;
import static org.epics.archiverappliance.common.FailoverTestUtil.updateLocalPVTypeInfo;

/**
 * Test basic failover - test the ETL side of things when the other server is down...
 * @author mshankar
 *
 */
class FailoverETLServerDownTest {
    private static final Logger logger = LogManager.getLogger(FailoverETLServerDownTest.class.getName());
    private ConfigServiceForTests configService;
    String pvName = "FailoverETLServerDownTest";
    long tCount = 0;
    long stepSeconds = 2;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    public void tearDown() throws Exception {}

    private void changeMTSForDest() throws Exception {
        String otherURL = "pbraw://localhost?name=MTS&rawURL="
                + URLEncoder.encode(
                        "http://localhost:" + FAILOVER_DEST_PORT + "/retrieval/data/getData.raw",
                        StandardCharsets.UTF_8);
        updateLocalPVTypeInfo(configService.getMyApplianceInfo().getIdentity(), configService, pvName, otherURL);
    }

    private long testMergedRetrieval(String pluginURL, Instant startTime, Instant endTime) throws Exception {
        long rtvlEventCount = 0;
        long lastEvEpoch = 0;
        StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(pluginURL, configService);
        try (BasicContext context = new BasicContext()) {
            List<Callable<EventStream>> callables =
                    plugin.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
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

        System.getProperties()
                .put(
                        "ARCHAPPL_SHORT_TERM_FOLDER",
                        "libs/" + this.getClass().getSimpleName() + "/" + FAILOVER_DEST_NAME + "/sts");
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", getRootFolderString() + "/mts");
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", getRootFolderString() + "/lts");

        tCount = generateData(
                FAILOVER_DEST_NAME,
                lastMonth,
                1,
                configService,
                pvName,
                stepSeconds,
                this.getClass().getSimpleName(),
                "MTS");

        changeMTSForDest();
        Instant timeETLruns = TimeUtils.plusDays(TimeUtils.now(), 365 * 10);
        logger.info("Running ETL now as if it is " + TimeUtils.convertToHumanReadableString(timeETLruns));
        ETLExecutor.runETLs(configService, timeETLruns);

        logger.info("Checking merged data after running ETL");
        long lCount = testMergedRetrieval(
                "pb://localhost?name=LTS&rootFolder=" + getRootFolderString() + "/lts"
                        + "&partitionGranularity=PARTITION_YEAR",
                TimeUtils.minusDays(TimeUtils.now(), 365 * 2),
                TimeUtils.plusDays(TimeUtils.now(), 365 * 2));
        Assertions.assertEquals(0, lCount, "We expected LTS to have failed " + lCount);
        long mCount = testMergedRetrieval(
                "pb://localhost?name=MTS&rootFolder=" + getRootFolderString() + "/mts"
                        + "&partitionGranularity=PARTITION_DAY",
                TimeUtils.minusDays(TimeUtils.now(), 365 * 2),
                TimeUtils.plusDays(TimeUtils.now(), 365 * 2));
        Assertions.assertEquals(
                mCount,
                tCount,
                "We expected MTS to have the same amount of data " + tCount + " instead we got " + mCount);
    }

    private String getRootFolderString() {
        return BUILD_TOMCATS_TOMCAT + this.getClass().getSimpleName() + "/" + FAILOVER_DEST_NAME;
    }
}
