/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import gov.aps.jca.dbr.DBR;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.utils.imprt.CSVEvent;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;

/**
 * Generates a file using appendData for one years worth of a DBT type and then runs a validation check afterwards.
 * @author mshankar
 *
 */
public class DBRTypeTest {
    private static final Logger logger = LogManager.getLogger(DBRTypeTest.class.getName());
    PBCommonSetup pbSetup = new PBCommonSetup();

    @AfterEach
    public void tearDown() throws Exception {
        pbSetup.deleteTestFolder();
    }
@Test
    public void testJCAPopulateAndRead() throws Exception {
        PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
        pbSetup.setUpRootFolder(pbplugin, "DBRTypeTests");

        ConfigService configService = new ConfigServiceForTests(new File("./bin"));
        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            if (!dbrType.isV3Type()) continue;
            logger.info("Testing JCA conversion for DBR_type: " + dbrType.name());
            BoundaryConditionsSimulationValueGenerator valuegenerator =
                    new BoundaryConditionsSimulationValueGenerator();
            for (int secondsintoyear = 0;
                    secondsintoyear < valuegenerator.getNumberOfSamples(dbrType);
                    secondsintoyear++) {
                try {
                    DBR dbr = valuegenerator.getJCASampleValue(dbrType, secondsintoyear);
                    Event e = configService
                            .getArchiverTypeSystem()
                            .getJCADBRConstructor(dbrType)
                            .newInstance(dbr);
                    SampleValue eexpectedval = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                    SampleValue actualValue = e.getSampleValue();
                    if (!eexpectedval.equals(actualValue)) {
                        Assertions.fail("Value mismatch found at " + secondsintoyear + " when testing " + dbrType
                                + ". Expecting " + eexpectedval + " of class "
                                + eexpectedval.getValue().getClass().getName()
                                + " found " + actualValue.toString() + " of class "
                                + actualValue.getValue().getClass().getName());
                        return;
                    }
                } catch (Exception ex) {
                    logger.error(ex.getMessage(), ex);
                    Assertions.fail("Exception at time = " + secondsintoyear + " when testing " + dbrType);
                }
            }
        }
        configService.shutdownNow();
    }
@Test
    @Tag("slow")
    public void testPopulateAndRead() throws Exception {
        PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
        pbSetup.setUpRootFolder(pbplugin, "DBRTypeTests");
        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            ConfigService configService = new ConfigServiceForTests(new File("./bin"));
            FileBackedPBEventStream retrievedStrm = null;
            try {
                BoundaryConditionsSimulationValueGenerator valuegenerator =
                        new BoundaryConditionsSimulationValueGenerator();
                // First write the data.
                logger.info("Generating DBR_type data for " + dbrType.name());
                SimulationEventStream simstream = new SimulationEventStream(dbrType, valuegenerator);
                try (BasicContext context = new BasicContext()) {
                    pbplugin.appendData(context, dbrType.name(), simstream);
                }
                logger.info("Done appending data. Now checking the read.");
                // Now test the data.
                long startOfCurrentYearInSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
                // EventStream retrievedStrm = pbplugin.getDataForPV(dbrType.name(),
                // TimeStamp.time(startOfCurrentYearInSeconds, 0),
                // TimeStamp.time(startOfCurrentYearInSeconds+SimulationEventStreamIterator.SECONDS_IN_YEAR, 0));
                Path path = PlainPBPathNameUtility.getPathNameForTime(
                        pbplugin,
                        dbrType.name(),
                        startOfCurrentYearInSeconds,
                        new ArchPaths(),
                        configService.getPVNameToKeyConverter());
                retrievedStrm = new FileBackedPBEventStream(dbrType.name(), path, dbrType);

                int expectedsecondsintoyear = 0;
                long expectedstartofyear = TimeUtils.getStartOfCurrentYearInSeconds();
                long start = System.currentTimeMillis();
                for (Event ev : retrievedStrm) {
                    int secondsintoyear =
                            TimeUtils.getSecondsIntoYear(ev.getEpochSeconds(), startOfCurrentYearInSeconds);
                    if (secondsintoyear != expectedsecondsintoyear) {
                        Assertions.fail("Mismatch found at " + secondsintoyear + " expecting " + expectedsecondsintoyear);
                    }

                    long actualstartofyear = TimeUtils.getStartOfYearInSeconds(ev.getEpochSeconds());
                    if (actualstartofyear != expectedstartofyear) {
                        Assertions.fail("Years are not matching at " + secondsintoyear + " expecting " + expectedstartofyear
                                + " found " + actualstartofyear);
                    }

                    SampleValue val = ev.getSampleValue();
                    SampleValue eexpectedval = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                    logger.debug("val is of type " + val.getClass().getName() + " and eexpectedval is of type "
                            + eexpectedval.getClass().getName());

                    if (!eexpectedval.equals(val)) {
                        Assertions.fail("Value mismatch found at " + secondsintoyear + " when testing " + dbrType
                                + ". Expecting " + eexpectedval
                                + " found " + val);
                        return;
                    }
                    expectedsecondsintoyear++;
                }
                long end = System.currentTimeMillis();
                logger.info("Checked " + expectedsecondsintoyear + " samples of DBR type " + dbrType.name() + " in "
                        + (end - start) + "(ms)");
                configService.shutdownNow();
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
    }

    @Test
    public void testCSVEvents() {
        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            if (dbrType == ArchDBRTypes.DBR_V4_GENERIC_BYTES) {
                // There is no sense is checking for CSV for DBR_V4_GENERIC_BYTES; this is a bytebuf anyways.
                logger.info("Skipping checking CSV conversion for V4 generic type");
                continue;
            }
            logger.info("Testing CSV events for DBR_type: " + dbrType.name());
            BoundaryConditionsSimulationValueGenerator valuegenerator =
                    new BoundaryConditionsSimulationValueGenerator();
            for (int secondsintoyear = 0;
                    secondsintoyear < valuegenerator.getNumberOfSamples(dbrType);
                    secondsintoyear++) {
                try {
                    SampleValue generatedVal = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                    String[] line = new String[5];
                    line[0] = Long.valueOf(TimeUtils.getStartOfCurrentYearInSeconds() + secondsintoyear)
                            .toString();
                    line[1] = Integer.valueOf(secondsintoyear).toString(); // nanos
                    line[2] = CSVEvent.toString(generatedVal, dbrType);
                    line[3] = "0"; // Status
                    line[4] = "0"; // Severity
                    try {
                        CSVEvent csvEvent = new CSVEvent(line, dbrType);
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
    }
@Test
    @Tag("slow")
    public void testMultipleYearDataForDoubles() throws Exception {
        PlainPBStoragePlugin pbplugin = new PlainPBStoragePlugin();
        pbSetup.setUpRootFolder(pbplugin, "DBRTypeTests");
        ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        ConfigService configService = new ConfigServiceForTests(new File("./bin"));
        for (short year = 1990; year < 3000; year += 10) {
            FileBackedPBEventStream retrievedStrm = null;
            try {
                BoundaryConditionsSimulationValueGenerator valuegenerator =
                        new BoundaryConditionsSimulationValueGenerator();
                // First write the data.
                logger.info("Generating DBR_type data for " + dbrType.name() + " for year " + year);
                SimulationEventStream simstream = new SimulationEventStream(dbrType, valuegenerator, year);
                try (BasicContext context = new BasicContext()) {
                    pbplugin.appendData(context, dbrType.name(), simstream);
                }
                logger.info("Done appending data. Now checking the read.");
                // Now test the data.
                long startOfYearInSeconds = TimeUtils.getStartOfYearInSeconds(year);
                // EventStream retrievedStrm = pbplugin.getDataForPV(dbrType.name(),
                // TimeStamp.time(startOfCurrentYearInSeconds, 0),
                // TimeStamp.time(startOfCurrentYearInSeconds+SimulationEventStreamIterator.SECONDS_IN_YEAR, 0));
                Path path = PlainPBPathNameUtility.getPathNameForTime(
                        pbplugin,
                        dbrType.name(),
                        startOfYearInSeconds,
                        new ArchPaths(),
                        configService.getPVNameToKeyConverter());
                retrievedStrm = new FileBackedPBEventStream(dbrType.name(), path, dbrType);

                int expectedsecondsintoyear = 0;
                long expectedstartofyear = TimeUtils.getStartOfYearInSeconds(year);
                long start = System.currentTimeMillis();
                for (Event ev : retrievedStrm) {
                    int secondsintoyear = TimeUtils.getSecondsIntoYear(ev.getEpochSeconds(), startOfYearInSeconds);
                    if (secondsintoyear != expectedsecondsintoyear) {
                        Assertions.fail("Mismatch found at " + secondsintoyear + " expecting " + expectedsecondsintoyear);
                    }

                    long actualstartofyear = TimeUtils.getStartOfYearInSeconds(ev.getEpochSeconds());
                    if (actualstartofyear != expectedstartofyear) {
                        Assertions.fail("Years are not matching at " + secondsintoyear + " expecting " + expectedstartofyear
                                + " found " + actualstartofyear);
                    }

                    SampleValue val = ev.getSampleValue();
                    SampleValue eexpectedval = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                    if (!eexpectedval.equals(val)) {
                        Assertions.fail("Value mismatch found at " + secondsintoyear + " when testing " + dbrType
                                + ". Expecting " + eexpectedval + " of class "
                                + eexpectedval.getValue().getClass().getName()
                                + " found " + val.toString() + " of class "
                                + val.getValue().getClass().getName());
                        return;
                    }
                    expectedsecondsintoyear++;
                }
                long end = System.currentTimeMillis();
                logger.info("Checked " + expectedsecondsintoyear + " samples of DBR type " + dbrType.name() + " in "
                        + (end - start) + "(ms)");
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
        configService.shutdownNow();
    }

    @Test
    @Tag("slow")
    public void testSetRepeatCount() {
        // Setting the repeat count un-marshals a PB message, merges it into a new object, sets the RepeatCount and
        // serializes it again.
        // We want to test that we do not lose information in this transformation

        for (ArchDBRTypes dbrType : ArchDBRTypes.values()) {
            if (!dbrType.isV3Type()) continue;
            logger.info("Testing setting repeat count for DBR_type: " + dbrType.name());
            BoundaryConditionsSimulationValueGenerator valuegenerator =
                    new BoundaryConditionsSimulationValueGenerator();
            for (int secondsintoyear = 0;
                    secondsintoyear < valuegenerator.getNumberOfSamples(dbrType);
                    secondsintoyear++) {
                try {
                    DBR dbr = valuegenerator.getJCASampleValue(dbrType, secondsintoyear);
                    DBRTimeEvent beforeEvent = EPICS2PBTypeMapping.getPBClassFor(dbrType)
                            .getJCADBRConstructor()
                            .newInstance(dbr);
                    beforeEvent.setRepeatCount(secondsintoyear);
                    ByteArray rawForm = beforeEvent.getRawForm();
                    YearSecondTimestamp yts = TimeUtils.convertToYearSecondTimestamp(beforeEvent.getEventTimeStamp());
                    DBRTimeEvent afterEvent = DBR2PBTypeMapping.getPBClassFor(dbrType)
                            .getUnmarshallingFromByteArrayConstructor()
                            .newInstance(yts.getYear(), rawForm);

                    SampleValue eexpectedval = valuegenerator.getSampleValue(dbrType, secondsintoyear);
                    SampleValue actualValue = afterEvent.getSampleValue();
                    if (!eexpectedval.equals(actualValue)) {
                        Assertions.fail("Value mismatch found at " + secondsintoyear + " when testing " + dbrType
                                + ". Expecting " + eexpectedval + " of class "
                                + eexpectedval.getValue().getClass().getName()
                                + " found " + actualValue.toString() + " of class "
                                + actualValue.getValue().getClass().getName());
                        return;
                    }

                    long beforeEpochSeconds = beforeEvent.getEpochSeconds();
                    long afterEpochSeconds = afterEvent.getEpochSeconds();
	                Assertions.assertEquals(afterEpochSeconds, beforeEpochSeconds, "RepeatCount beforeEpochSeconds=" + beforeEpochSeconds + " afterEpochSeconds="
			                + afterEpochSeconds);

                    long beforeNanos = beforeEvent.getEventTimeStamp().getNanos();
                    long afterNanos = afterEvent.getEventTimeStamp().getNanos();
                    Assertions.assertEquals(afterNanos, beforeNanos,"RepeatCount beforeNanos=" + beforeNanos + " afterNanos=" + afterNanos);

                    Assertions.assertEquals(secondsintoyear, afterEvent.getRepeatCount(), "RepeatCount seems to not have been set to " + secondsintoyear);

                } catch (Exception ex) {
                    logger.error(ex.getMessage(), ex);
                    Assertions.fail("Exception at time = " + secondsintoyear + " when testing " + dbrType);
                }
            }
        }
    }
}
