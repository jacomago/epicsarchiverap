package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.plain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

/**
 * Unit test to test for timezones that are ahead of UTC; use "Australia/Sydney".
 * We generate a few partitions worth of data and then see that data is moving over as time goes by.
 * @author mshankar
 *
 */
public class TimezoneAheadTest {
    private static final Logger logger = LogManager.getLogger(TimezoneAheadTest.class.getName());

    public static Stream<Arguments> provideArguments() {
        return ETLTestPlugins.generateFileExtensions().stream()
                .flatMap(fPair -> Stream.of(
                        Arguments.of(
                                PartitionGranularity.PARTITION_HOUR,
                                PartitionGranularity.PARTITION_DAY,
                                fPair[0],
                                fPair[1]),
                        Arguments.of(
                                PartitionGranularity.PARTITION_DAY,
                                PartitionGranularity.PARTITION_DAY,
                                fPair[0],
                                fPair[1])));
    }

    @BeforeEach
    public void setUp() throws Exception {
        System.getProperties().put("user.timezone", "Australia/Sydney");
    }

    @ParameterizedTest
    @MethodSource("provideArguments")
    public void testETLMoveForPartitionGranularity(
            PartitionGranularity srcGranularity,
            PartitionGranularity destGranularity,
            FileExtension stsFileExtension,
            FileExtension mtsFileExtension)
            throws Exception {
        logger.debug(TimeUtils.convertToHumanReadableString(TimeUtils.now()));

        PlainStoragePlugin etlSrc = new PlainStoragePlugin(stsFileExtension);
        PBCommonSetup srcSetup = new PBCommonSetup();
        PlainStoragePlugin etlDest = new PlainStoragePlugin(mtsFileExtension);
        PBCommonSetup destSetup = new PBCommonSetup();
        DefaultConfigService configService = new ConfigServiceForTests(new File("./bin"), 1);

        srcSetup.setUpRootFolder(etlSrc, "TimeZoneAheadETLTestSrc_" + srcGranularity, srcGranularity);
        destSetup.setUpRootFolder(
                etlDest, "TimeZoneAheadETLTestDest" + srcGranularity, destGranularity);

        long nowEpochSeconds = TimeUtils.getCurrentEpochSeconds();
        long startEpochSeconds = nowEpochSeconds - 10L * srcGranularity.getApproxSecondsPerChunk();
        long endEpochSeconds = nowEpochSeconds + 10L * srcGranularity.getApproxSecondsPerChunk();

        String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "ETL_testTimeZoneAhead"
                + etlSrc.getPartitionGranularity();

        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
        String[] dataStores = new String[] {etlSrc.getURLRepresentation(), etlDest.getURLRepresentation()};
        typeInfo.setDataStores(dataStores);
        configService.updateTypeInfoForPV(pvName, typeInfo);
        configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
        configService.getETLLookup().manualControlForUnitTests();

        {
            long eventSeconds = startEpochSeconds;
            while (eventSeconds <= endEpochSeconds) {
                int eventsPerShot = 10 * 1024;
                ArrayListEventStream instream = new ArrayListEventStream(
                        eventsPerShot,
                        new RemotableEventStreamDesc(
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                pvName,
                                TimeUtils.computeYearForEpochSeconds(eventSeconds)));
                for (int i = 0; i < eventsPerShot; i++) {
                    instream.add(new SimulationEvent(
                            TimeUtils.convertToYearSecondTimestamp(eventSeconds),
                            ArchDBRTypes.DBR_SCALAR_DOUBLE,
                            new ScalarValue<Number>(eventSeconds)));
                    eventSeconds++;
                }
                try (BasicContext context = new BasicContext()) {
                    etlSrc.appendData(context, pvName, instream);
                }
            }
        }

        {
            long eventSeconds = startEpochSeconds + srcGranularity.getApproxSecondsPerChunk();
            while (eventSeconds <= endEpochSeconds) {
                Path[] srcPathsBefore = PathNameUtility.getAllPathsForPV(
                        new ArchPaths(),
                        etlSrc.getRootFolder(),
                        pvName,
                        stsFileExtension.getExtensionString(),
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter());
                Path[] destPathsBefore = PathNameUtility.getAllPathsForPV(
                        new ArchPaths(),
                        etlDest.getRootFolder(),
                        pvName,
                        mtsFileExtension.getExtensionString(),
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter());
                Instant srcBeforeEpochSeconds = Instant.EPOCH;

                if (srcPathsBefore.length > 0) {
                    srcBeforeEpochSeconds = FileInfo.extensionPath(stsFileExtension, srcPathsBefore[0])
                            .getFirstEventInstant();
                }

                ETLExecutor.runETLs(configService, TimeUtils.convertFromEpochSeconds(eventSeconds, 0));

                Path[] srcPathsAfter = PathNameUtility.getAllPathsForPV(
                        new ArchPaths(),
                        etlSrc.getRootFolder(),
                        pvName,
                        stsFileExtension.getExtensionString(),
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter());
                Path[] destPathsAfter = PathNameUtility.getAllPathsForPV(
                        new ArchPaths(),
                        etlDest.getRootFolder(),
                        pvName,
                        mtsFileExtension.getExtensionString(),
                        CompressionMode.NONE,
                        configService.getPVNameToKeyConverter());

                logger.info("Running ETL at " + TimeUtils.convertToHumanReadableString(eventSeconds)
                        + " Before " + srcPathsBefore.length + "/" + destPathsBefore.length
                        + " After " + srcPathsAfter.length + "/" + destPathsAfter.length);

                Instant srcAfterEpochSeconds = Instant.EPOCH;

                if (srcPathsAfter.length > 0) {
                    srcAfterEpochSeconds = FileInfo.extensionPath(stsFileExtension, srcPathsAfter[0])
                            .getFirstEventInstant();
                }

                if (srcAfterEpochSeconds.isAfter(Instant.EPOCH) && srcBeforeEpochSeconds.isAfter(Instant.EPOCH)) {
                    Assertions.assertTrue(
                            srcAfterEpochSeconds.isAfter(srcBeforeEpochSeconds),
                            "The first event in the source after ETL "
                                    + srcAfterEpochSeconds
                                    + " should be greater then the first event in the source before ETL"
                                    + srcBeforeEpochSeconds);
                } else {
                    logger.warn("ETL did not move data at " + TimeUtils.convertToHumanReadableString(eventSeconds));
                }

                eventSeconds = eventSeconds + srcGranularity.getApproxSecondsPerChunk();
            }
        }
        srcSetup.deleteTestFolder();
        destSetup.deleteTestFolder();
    }
}
