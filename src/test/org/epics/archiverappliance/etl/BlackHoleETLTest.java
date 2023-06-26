/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.FileExtension;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.blackhole.BlackholeStoragePlugin;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Test pouring data into the black hole plugin.
 * @author mshankar
 *
 */
public class BlackHoleETLTest {
    private static final Logger logger = LogManager.getLogger(BlackHoleETLTest.class.getName());
    private ConfigService configService;
    int ratio = 10; // Size of the test

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"));
    }

    /**
     * Variant of the gradual accumulation test where the destination is a black hole plugin
     */
    public static Stream<Arguments> provideBlackHoleETL() {
        return Arrays.stream(PartitionGranularity.values())
                .filter(g -> g.getNextLargerGranularity() != null)
                .flatMap(g -> Arrays.stream(FileExtension.values())
                        .flatMap(f -> Stream.of(Arguments.of(g, f))));
    }

    @ParameterizedTest
    @MethodSource("provideBlackHoleETL")
    public void testBlackHoleETL(PartitionGranularity granularity, FileExtension fileExtension)
            throws Exception {

        PlainPBStoragePlugin etlSrc = new PlainPBStoragePlugin(fileExtension);
        PBCommonSetup srcSetup = new PBCommonSetup();
        BlackholeStoragePlugin etlDest = new BlackholeStoragePlugin();

        srcSetup.setUpRootFolder(etlSrc, "BlackholeETLTestSrc_" + granularity, granularity, fileExtension);

        logger.info("Testing black hole for " + etlSrc.getPartitionGranularity() + " to "
                + etlDest.getPartitionGranularity());

        short year = TimeUtils.getCurrentYear();
        long curEpochSeconds = TimeUtils.getStartOfCurrentYearInSeconds();
        int secondsInTestingPeriod = 0;
        int incrementSeconds = granularity.getApproxSecondsPerChunk() / ratio;

        String pvName =
                ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_blackhole" + etlSrc.getPartitionGranularity();

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        try (BasicContext context = new BasicContext()) {
            while (secondsInTestingPeriod < granularity.getApproxSecondsPerChunk() * ratio) {
                ArrayListEventStream instream = new ArrayListEventStream(
                        incrementSeconds, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                for (int i = 0; i < ratio; i++) {
                    instream.add(new SimulationEvent(
                            secondsInTestingPeriod, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double)
                            secondsInTestingPeriod)));
                    secondsInTestingPeriod += incrementSeconds;
                    curEpochSeconds += incrementSeconds;
                }
                etlSrc.appendData(context, pvName, instream);
                int filesWithDataBefore = getFilesWithData(pvName, etlSrc, fileExtension);
                ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(curEpochSeconds, 0));
                logger.debug("Done performing ETL");
                int filesWithDataAfter = getFilesWithData(pvName, etlSrc, fileExtension);
                Assertions.assertTrue(
                        filesWithDataAfter < filesWithDataBefore,
                        "Black hole did not remove source files before = " + filesWithDataBefore + " and after = "
                                + filesWithDataAfter + " for granularity " + granularity);
            }
        }

        srcSetup.deleteTestFolder();
    }

    private int getFilesWithData(String pvName, PlainPBStoragePlugin etlSrc, FileExtension fileExtension) throws Exception {
        // Check that all the files in the destination store are valid files.
        Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(
                new ArchPaths(),
                etlSrc.getRootFolder(),
                pvName,
                fileExtension.getExtensionString(),
                etlSrc.getPartitionGranularity(),
                CompressionMode.NONE,
                configService.getPVNameToKeyConverter());
        return allPaths.length;
    }
}
