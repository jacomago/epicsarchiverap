/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PlainCommonSetup;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.HashMap;

/**
 * Test retrieval across year spans.
 * @author mshankar
 *
 */
@Tag("integration")
class YearSpanPPRetrievalTest {
    private static final Logger logger = LogManager.getLogger(YearSpanPPRetrievalTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    private ConfigService configService;

    @BeforeEach
    void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    private void generateDataForYears(PlainStoragePlugin pbplugin, String pvName) throws IOException {
        // We skip generation of the file only if all the files exist.
        boolean deletefilesandgeneratedata = false;
        for (short currentyear = (short) 2010; currentyear <= (short) 2013; currentyear++) {
            if (!PathNameUtility.getPathNameForTime(
                            pbplugin,
                            pvName,
                            TimeUtils.getStartOfYear(currentyear),
                            new ArchPaths(),
                            configService.getPVNameToKeyConverter())
                    .toFile()
                    .exists()) {
                logger.info("File for year " + currentyear + " does not exist. Generating data for all the years.");
                deletefilesandgeneratedata = true;
                break;
            }
        }
        // Delete all the files for the specified span
        if (deletefilesandgeneratedata) {
            for (short currentyear = (short) 2010; currentyear <= (short) 2013; currentyear++) {
                Files.deleteIfExists(PathNameUtility.getPathNameForTime(
                        pbplugin,
                        pvName,
                        TimeUtils.getStartOfYear(currentyear),
                        new ArchPaths(),
                        configService.getPVNameToKeyConverter()));
            }

            SimulationEventStream simstream = new SimulationEventStream(
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new SineGenerator(0),
                    TimeUtils.getStartOfYear(2010),
                    TimeUtils.getEndOfYear(2013),
                    1);
            // The pbplugin should handle all the rotation etc.
            try (BasicContext context = new BasicContext()) {
                pbplugin.appendData(context, pvName, simstream);
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    static String wrapPVName(String pvName, String pp) {
        return pp + "(" + pvName + ")";
    }

    @Test
    void testYearSpan() throws Exception {
        PlainCommonSetup pbSetup = new PlainCommonSetup();
        PlainStoragePlugin pbplugin = new PlainStoragePlugin();
        pbSetup.setUpRootFolder(pbplugin);
        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "yspanpp";
        generateDataForYears(pbplugin, pvName);
        RawDataRetrievalAsEventStream rawDataRetrieval =
                new RawDataRetrievalAsEventStream(ConfigServiceForTests.RAW_RETRIEVAL_URL);
        Instant start = TimeUtils.convertFromISO8601String("2011-12-31T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String("2013-01-01T08:00:00.000Z");
        EventStream stream = null;
        try {
            stream = rawDataRetrieval.getDataForPVS(
                    new String[] {wrapPVName(pvName, "optimized_5760")},
                    start,
                    end,
                    desc -> logger.info("On the client side, switching to processing PV " + desc.getPvName()));

            // We are making sure that the stream we get back has times in sequential order...

            HashMap<Short, YearCount> counts = new HashMap<Short, YearCount>();
            Instant previousTime = Instant.EPOCH;

            for (Event e : stream) {
                Instant actualTime = e.getEventTimeStamp();
                Assertions.assertTrue(actualTime.isAfter(previousTime));
                previousTime = actualTime;

                YearSecondTimestamp actualYST = TimeUtils.convertToYearSecondTimestamp(actualTime);
                YearCount ycount = counts.get(actualYST.getYear());
                if (ycount == null) {
                    ycount = new YearCount();
                    counts.put(actualYST.getYear(), ycount);
                }
                ycount.count++;
            }

            Assertions.assertTrue(counts.get((short) 2011).count > 20000);
            Assertions.assertTrue(counts.get((short) 2012).count > 20000);
        } finally {
            if (stream != null)
                try {
                    stream.close();
                    stream = null;
                } catch (Throwable ignored) {
                }
        }
        pbSetup.deleteTestFolder();
    }

    static class YearCount {
        int count = 0;
    }
}
