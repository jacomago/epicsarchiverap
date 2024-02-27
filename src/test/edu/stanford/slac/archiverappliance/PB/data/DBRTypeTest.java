/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import edu.stanford.slac.archiverappliance.PlainPB.FileStreamCreator;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import gov.aps.jca.dbr.DBR;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Generates a file using appendData for one years worth of a DBT type and then runs a validation check afterwards.
 * @author mshankar
 *
 */
public class DBRTypeTest {
    private static final Logger logger = LogManager.getLogger(DBRTypeTest.class.getName());
    static PlainPBStoragePlugin pbplugin;
    static PBCommonSetup pbSetup = new PBCommonSetup();
    static ConfigService configService;

    private static final int SECONDS_INTO_YEAR = 100;

    @BeforeAll
    public static void setUp() throws Exception {
        pbplugin = new PlainPBStoragePlugin();
        pbSetup.setUpRootFolder(pbplugin, "DBRTypeTestsPB");
        configService = new ConfigServiceForTests(-1);
    }

    @AfterAll
    public static void tearDownAll() {

        configService.shutdownNow();
    }

    @AfterEach
    public void tearDown() throws Exception {
        pbSetup.deleteTestFolder();
    }

    static Stream<ArchDBRTypes> provideFileExtensionDBRType() {
        return Arrays.stream(ArchDBRTypes.values());
    }

    @ParameterizedTest
    @EnumSource(ArchDBRTypes.class)
    public void testJCAPopulateAndRead(ArchDBRTypes dbrType) {

        if (!dbrType.isV3Type()) return;
        logger.info("Testing JCA conversion for DBR_type: " + dbrType.name());
        BoundaryConditionsSimulationValueGenerator valuegenerator = new BoundaryConditionsSimulationValueGenerator();
        for (int secondsintoyear = 0; secondsintoyear < SECONDS_INTO_YEAR; secondsintoyear++) {
            try {
                DBR dbr = valuegenerator.getJCASampleValue(dbrType, secondsintoyear);
                Event e = DefaultEvent.fromDBR(dbrType, dbr, Map.of(), false);
                SampleValue eexpectedval = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                SampleValue actualValue = e.getSampleValue();
                Assertions.assertEquals(eexpectedval, actualValue);
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                Assertions.fail("Exception at time = " + secondsintoyear + " when testing " + dbrType);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideFileExtensionDBRType")
    public void testPopulateAndRead(ArchDBRTypes dbrType) {
        EventStream retrievedStrm = null;
        try {
            BoundaryConditionsSimulationValueGenerator valuegenerator =
                    new BoundaryConditionsSimulationValueGenerator();
            // First write the data.
            logger.info("Generating DBR_type data for " + dbrType.name());
            short currentYear = TimeUtils.getCurrentYear();
            Instant startTime = TimeUtils.getStartOfYear(currentYear);
            Instant endTime = TimeUtils.getEndOfYear(currentYear);
            int periodInSeconds = 10000;
            SimulationEventStream simstream =
                    new SimulationEventStream(dbrType, valuegenerator, startTime, endTime, periodInSeconds);
            String pvName = "testPopulateAndRead" + dbrType.name();
            try (BasicContext context = new BasicContext()) {
                pbplugin.appendData(context, pvName, simstream);
            }
            logger.info("Done appending data. Now checking the read.");
            // Now test the data.
            Path path = PlainPBPathNameUtility.getPathNameForTime(
                    pbplugin,
                    pvName,
                    TimeUtils.getStartOfYear(currentYear),
                    new ArchPaths(),
                    configService.getPVNameToKeyConverter());
            retrievedStrm = FileStreamCreator.getStream(pvName, path, dbrType);

            Instant expectedTime = startTime;
            long start = System.currentTimeMillis();
            for (Event ev : retrievedStrm) {
                Instant evTimestamp = ev.getEventTimeStamp();
                Assertions.assertEquals(evTimestamp, expectedTime);

                SampleValue val = ev.getSampleValue();
                SampleValue eexpectedval = valuegenerator.getSampleValue(
                        dbrType, TimeUtils.getSecondsIntoYear(expectedTime.getEpochSecond()));
                logger.debug("val is of type " + val.getClass().getName() + " and eexpectedval is of type "
                        + eexpectedval.getClass().getName());

                Assertions.assertEquals(eexpectedval, val);
                expectedTime = expectedTime.plusSeconds(periodInSeconds);
            }
            long end = System.currentTimeMillis();
            logger.info("Checked " + Duration.between(startTime, expectedTime).getSeconds() / periodInSeconds
                    + " samples of DBR type " + dbrType.name() + " in " + (end - start) + "(ms)");
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            Assertions.fail(ex.getMessage());
        } finally {
            try {
                retrievedStrm.close();
            } catch (Throwable ignored) {
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ArchDBRTypes.class)
    public void testCSVEvents(ArchDBRTypes dbrType) {
        if (dbrType == ArchDBRTypes.DBR_V4_GENERIC_BYTES) {
            // There is no sense is checking for CSV for DBR_V4_GENERIC_BYTES; this is a bytebuf anyways.
            logger.info("Skipping checking CSV conversion for V4 generic type");
            return;
        }
        logger.info("Testing CSV events for DBR_type: " + dbrType.name());
        BoundaryConditionsSimulationValueGenerator valuegenerator = new BoundaryConditionsSimulationValueGenerator();
        for (int secondsintoyear = 0; secondsintoyear < SECONDS_INTO_YEAR; secondsintoyear++) {
            try {
                SampleValue generatedVal = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                String[] line = new String[5];
                line[0] = Long.valueOf(TimeUtils.getStartOfCurrentYearInSeconds() + secondsintoyear)
                        .toString();
                line[1] = Integer.valueOf(secondsintoyear).toString(); // nanos
                line[2] = SampleValue.toCSVString(generatedVal, dbrType);
                line[3] = "0"; // Status
                line[4] = "0"; // Severity
                try {
                    DefaultEvent<?> csvEvent = DefaultEvent.fromCSV(line, dbrType);
                    SampleValue convertedValue = csvEvent.getSampleValue();
                    if (!convertedValue.equals(generatedVal)) {
                        Assertions.fail("Value mismatch found at " + secondsintoyear + " when testing " + dbrType
                                + ". Expecting " + generatedVal.toString()
                                + " found " + convertedValue);
                        return;
                    }
                } catch (Exception ex) {
                    logger.error(ex.getMessage(), ex);
                    Assertions.fail(ex.getMessage());
                }
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                Assertions.fail("Exception at time = " + secondsintoyear + " when testing " + dbrType);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideFileExtensionDBRType")
    public void testMultipleYearDataForDoubles(ArchDBRTypes dbrType) {
        for (short year = 1990; year < 3000; year += 10) {
            EventStream retrievedStrm = null;
            try {
                BoundaryConditionsSimulationValueGenerator valuegenerator =
                        new BoundaryConditionsSimulationValueGenerator();
                // First write the data.
                logger.info("Generating DBR_type data for " + dbrType.name() + " for year " + year);
                Instant startTime = TimeUtils.getStartOfYear(year);
                Instant endTime = TimeUtils.getEndOfYear(year);
                int periodInSeconds = PartitionGranularity.PARTITION_MONTH.getApproxSecondsPerChunk();
                String pvName = "testMultipleYearDataForDoubles" + dbrType.name();
                SimulationEventStream simstream =
                        new SimulationEventStream(dbrType, valuegenerator, startTime, endTime, periodInSeconds);
                try (BasicContext context = new BasicContext()) {
                    pbplugin.appendData(context, pvName, simstream);
                }
                logger.info("Done appending data. Now checking the read.");
                // Now test the data.
                // EventStream retrievedStrm = pbplugin.getDataForPV(dbrType.name(),
                // TimeStamp.time(startOfCurrentYearInSeconds, 0),
                // TimeStamp.time(startOfCurrentYearInSeconds+SimulationEventStreamIterator.SECONDS_IN_YEAR, 0));
                Path path = PlainPBPathNameUtility.getPathNameForTime(
                        pbplugin, pvName, startTime, new ArchPaths(), configService.getPVNameToKeyConverter());
                retrievedStrm = FileStreamCreator.getStream(pvName, path, dbrType);

                Instant expectedTime = startTime;
                long start = System.currentTimeMillis();
                for (Event ev : retrievedStrm) {
                    Instant evTimestamp = ev.getEventTimeStamp();
                    Assertions.assertEquals(evTimestamp, expectedTime);

                    SampleValue val = ev.getSampleValue();
                    SampleValue eexpectedval = valuegenerator.getSampleValue(
                            dbrType, TimeUtils.getSecondsIntoYear(expectedTime.getEpochSecond()));
                    Assertions.assertEquals(eexpectedval, val);
                    expectedTime = expectedTime.plusSeconds(periodInSeconds);
                }
                long end = System.currentTimeMillis();
                logger.info(
                        "Checked " + Duration.between(startTime, expectedTime).getSeconds() / periodInSeconds
                                + " samples of DBR type " + dbrType.name() + " in " + (end - start) + "(ms)");
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                Assertions.fail(ex.getMessage());
            } finally {
                try {
                    retrievedStrm.close();
                } catch (Throwable t) {
                }
            }
        }
    }
}
