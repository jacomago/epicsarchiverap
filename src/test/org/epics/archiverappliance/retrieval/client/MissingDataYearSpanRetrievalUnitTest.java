/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.plain.FileExtension;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;

/**
 * Test retrieval across year spans when some of the data is missing.
 * We generate data for these time periods
 * <ol>
 * <li>Sep 2011 - Oct 2011</li>
 * <li>Jun 2012 - Jul 2012</li>
 * </ol>
 * <p>
 * We then make requests for various time periods and check the first sample and number of samples.
 *
 * @author mshankar
 */
class MissingDataYearSpanRetrievalUnitTest {
    private static final Logger logger = LogManager.getLogger(MissingDataYearSpanRetrievalUnitTest.class.getName());
    private final LinkedList<Instant> generatedTimeStamps = new LinkedList<Instant>();
    String testSpecificFolder = "MissingDataYearSpanRetrieval";
    String pvNamePB = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + FileExtension.PB + ":" + testSpecificFolder
            + ":mdata_yspan";
    String pvNameParquet = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + FileExtension.PARQUET + ":"
            + testSpecificFolder + ":mdata_yspan";
    File dataFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ArchUnitTest"
            + File.separator + testSpecificFolder);
    PBCommonSetup PBSetup = new PBCommonSetup();
    PBCommonSetup parquetSetup = new PBCommonSetup();
    PlainStoragePlugin pbPlugin = new PlainStoragePlugin(FileExtension.PB);
    PlainStoragePlugin parquetPlugin = new PlainStoragePlugin(FileExtension.PARQUET);

    @BeforeEach
    public void setUp() throws Exception {
        PBSetup.setUpRootFolder(pbPlugin);
        parquetSetup.setUpRootFolder(parquetPlugin);
        logger.info("Data folder is " + dataFolder.getAbsolutePath());
        FileUtils.deleteDirectory(dataFolder);
        generateData();
    }

    private void generateData() throws IOException {
        {
            // Generate some data for Sep 2011 - Oct 2011, one per day
            Instant sep2011 = TimeUtils.convertFromISO8601String("2011-09-01T00:00:00.000Z");
            int sep201101secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(sep2011));
            short year = 2011;
            generateData(year, sep201101secsIntoYear);
        }

        {
            // Generate some data for Jun 2012 - Jul 2012, one per day
            Instant jun2012 = TimeUtils.convertFromISO8601String("2012-06-01T00:00:00.000Z");
            int jun201201secsIntoYear = TimeUtils.getSecondsIntoYear(TimeUtils.convertToEpochSeconds(jun2012));
            short year = 2012;
            generateData(year, jun201201secsIntoYear);
        }
    }

    private void generateData(short year, int jun201201secsIntoYear) throws IOException {
        ArrayListEventStream strmPB = new ArrayListEventStream(
                0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvNamePB, year));
        ArrayListEventStream strmParquet = new ArrayListEventStream(
                0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvNameParquet, year));
        for (int day = 0; day < 30; day++) {
            YearSecondTimestamp yts = new YearSecondTimestamp(
                    year,
                    jun201201secsIntoYear + day * PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    111000000);
            strmPB.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
            strmParquet.add(new SimulationEvent(yts, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.0)));
            generatedTimeStamps.add(TimeUtils.convertFromYearSecondTimestamp(yts));
        }
        try (BasicContext context = new BasicContext()) {
            pbPlugin.appendData(context, pvNamePB, strmPB);
        }
        try (BasicContext context = new BasicContext()) {
            parquetPlugin.appendData(context, pvNameParquet, strmParquet);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(dataFolder);
    }

    /**
     * <pre>
     * .....Sep,2011.....Oct,2011..............Jan,1,2012..........Jun,2012......Jul,2012...............Dec,2012......
     * [] - should return no data
     * ...[.....] should return data whose first value should be Sep 1, 2011
     * ............[.....] should return data whose first value is start time - 1
     * .................[...........] should return data whose first value is start time - 1
     * ...................................[.] should return one sample for the last day of Sept, 2011
     * ...................................[...................] should return one sample for the last day of Sept, 2011
     * ................................................[..]  should return one sample for the last day of Sept, 2011
     * ................................................[...............]  should return may samples with the first sample as the last day of Sept, 2011
     * ..................................................................[......]  should return may samples all from 2012
     * ..........................................................................................[..] should return one sample for the last day of Jun, 2012
     * ...........................................................................................................................[..] should return one sample for the last day of Jun, 2012
     * <pre>
     */
    @Test
    void testMissingDataYearSpan() {

        for (String pvName : new String[]{pvNamePB, pvNameParquet}) {
            testRetrieval(
                    "2011-06-01T00:00:00.000Z", "2011-07-01T00:00:00.000Z", 0, null, -1, "Before all data", pvName);
            testRetrieval(
                    "2011-08-10T00:00:00.000Z",
                    "2011-09-15T10:00:00.000Z",
                    15,
                    "2011-09-01T00:00:00.111Z",
                    0,
                    "Aug/10/2011 - Sep/15/2011",
                    pvName);
            testRetrieval(
                    "2011-09-10T00:00:00.000Z",
                    "2011-09-15T10:00:00.000Z",
                    6,
                    "2011-09-09T00:00:00.111Z",
                    8,
                    "Sep/10/2011 - Sep/15/2011",
                    pvName);
            testRetrieval(
                    "2011-09-10T00:00:00.000Z",
                    "2011-10-15T10:00:00.000Z",
                    22,
                    "2011-09-09T00:00:00.111Z",
                    8,
                    "Sep/10/2011 - Oct/15/2011",
                    pvName);
            testRetrieval(
                    "2011-10-10T00:00:00.000Z",
                    "2011-10-15T10:00:00.000Z",
                    1,
                    "2011-09-30T00:00:00.111Z",
                    29,
                    "Oct/10/2011 - Oct/15/2011",
                    pvName);
            testRetrieval(
                    "2011-10-10T00:00:00.000Z",
                    "2012-01-15T10:00:00.000Z",
                    1,
                    "2011-09-30T00:00:00.111Z",
                    29,
                    "Oct/10/2011 - Jan/15/2012",
                    pvName);
            testRetrieval(
                    "2012-01-10T00:00:00.000Z",
                    "2012-01-15T10:00:00.000Z",
                    1,
                    "2011-09-30T00:00:00.111Z",
                    29,
                    "Jan/10/2012 - Jan/15/2012",
                    pvName);
            testRetrieval(
                    "2012-01-10T00:00:00.000Z",
                    "2012-06-15T10:00:00.000Z",
                    16,
                    "2011-09-30T00:00:00.111Z",
                    29,
                    "Jan/10/2012 - Jun/15/2012",
                    pvName);
            testRetrieval(
                    "2012-06-10T00:00:00.000Z",
                    "2012-06-15T10:00:00.000Z",
                    6,
                    "2012-06-09T00:00:00.111Z",
                    38,
                    "Jun/10/2012 - Jun/15/2012",
                    pvName);
            testRetrieval(
                    "2012-09-10T00:00:00.000Z",
                    "2012-09-15T10:00:00.000Z",
                    1,
                    "2012-06-30T00:00:00.111Z",
                    59,
                    "Sep/10/2012 - Sep/15/2012",
                    pvName);
            testRetrieval(
                    "2013-01-10T00:00:00.000Z",
                    "2013-01-15T10:00:00.000Z",
                    1,
                    "2012-06-30T00:00:00.111Z",
                    59,
                    "Jan/10/2013 - Jan/15/2013",
                    pvName);
        }
    }

    /**
     * @param startStr                  - Start time of request
     * @param endStr                    - End time of request
     * @param expectedMinEventCount     - How many events we expect at a minimum
     * @param firstTimeStampExpectedStr - The time stamp of the first event
     * @param firstTSIndex              - If present, the index into generatedTimeStamps for the first event. Set to -1 if you want to skip this check.
     * @param msg                       - msg to add to log msgs and the like
     */
    private void testRetrieval(
            String startStr,
            String endStr,
            int expectedMinEventCount,
            String firstTimeStampExpectedStr,
            int firstTSIndex,
            String msg,
            String pvName) {
        logger.info("testRerieval: {} {} {}",startStr, endStr, pvName);
        Instant start = TimeUtils.convertFromISO8601String(startStr);
        Instant end = TimeUtils.convertFromISO8601String(endStr);
        Instant firstTimeStampExpected = null;
        if (firstTimeStampExpectedStr != null) {
            firstTimeStampExpected = TimeUtils.convertFromISO8601String(firstTimeStampExpectedStr);
        }
        if (firstTSIndex != -1) {
            Assertions.assertEquals(
                    firstTimeStampExpected,
                    generatedTimeStamps.get(firstTSIndex),
                    "Incorrect specification - Str is " + firstTimeStampExpectedStr + " and from array "
                            + TimeUtils.convertToISO8601String(generatedTimeStamps.get(firstTSIndex)) + " for " + msg);
        }
        Instant obtainedFirstSample = null;
        int eventCount = 0;

        try(EventStream stream = new CurrentThreadWorkerEventStream(
                pvName, pbPlugin.getDataForPV(new BasicContext(), pvName, start, end, new DefaultRawPostProcessor()))) {


            for (Event e : stream) {
                if (obtainedFirstSample == null) {
                    obtainedFirstSample = e.getEventTimeStamp();
                }
                Assertions.assertEquals(
                        e.getEventTimeStamp(),
                        generatedTimeStamps.get(firstTSIndex + eventCount),
                        "Expecting sample with timestamp "
                                + TimeUtils.convertToISO8601String(
                                generatedTimeStamps.get(firstTSIndex + eventCount))
                                + " got "
                                + TimeUtils.convertToISO8601String(e.getEventTimeStamp())
                                + " for " + msg);
                eventCount++;
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }


        Assertions.assertTrue(
                eventCount >= expectedMinEventCount,
                "Expecting at least " + expectedMinEventCount + " got " + eventCount + " for " + msg);
        if (firstTimeStampExpected != null) {
            if (obtainedFirstSample == null) {
                Assertions.fail("Expecting at least one value for " + msg);
            } else {
                Assertions.assertEquals(
                        firstTimeStampExpected,
                        obtainedFirstSample,
                        "Expecting first sample to be "
                                + TimeUtils.convertToISO8601String(firstTimeStampExpected)
                                + " got "
                                + TimeUtils.convertToISO8601String(obtainedFirstSample)
                                + " for " + msg);
            }
        } else {
            if (obtainedFirstSample != null) {
                Assertions.fail("Expecting no values for " + msg + " Got value from "
                        + TimeUtils.convertToISO8601String(obtainedFirstSample));
            }
        }
    }
}
