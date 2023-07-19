package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility.StartEndTimeFromName;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.simulation.SimulationEventStream;
import org.epics.archiverappliance.utils.simulation.SineGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

/**
 * Tests that the PB plugin appendData stores data in clean partitions.
 * That is, the events do not leak into before or after the times as determined from the partition names
 * @author mshankar
 *
 */
public class CleanPartitionsTest {
    private static final Logger logger = LogManager.getLogger(CleanPartitionsTest.class.getName());
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(new File("./bin"));
    }

    @Test
    public void testCleanPartitions() throws Exception {
        for (PartitionGranularity granularity : PartitionGranularity.values()) {
            PlainPBStoragePlugin pbPlugin = new PlainPBStoragePlugin();
            PBCommonSetup srcSetup = new PBCommonSetup();

            srcSetup.setUpRootFolder(pbPlugin, "TestCleanPartitions_" + granularity, granularity);

            String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "CleanPartition"
                    + pbPlugin.getPartitionGranularity();
            SimulationEventStream simstream =
                    new SimulationEventStream(ArchDBRTypes.DBR_SCALAR_DOUBLE, new SineGenerator(0));
            try (BasicContext context = new BasicContext()) {
                pbPlugin.appendData(context, pvName, simstream);
            }
            logger.info("Done creating src data for PV " + pvName + " for granularity "
                    + pbPlugin.getPartitionGranularity());

            Path[] allPaths = PlainPBPathNameUtility.getAllPathsForPV(
                    new ArchPaths(),
                    pbPlugin.getRootFolder(),
                    pvName,
                    ".pb",
                    pbPlugin.getPartitionGranularity(),
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
            for (Path pbFile : allPaths) {
                PBFileInfo fileInfo = new PBFileInfo(pbFile);
                StartEndTimeFromName chunkTimes = PlainPBPathNameUtility.determineTimesFromFileName(
                        pvName,
                        pbFile.getFileName().toString(),
                        pbPlugin.getPartitionGranularity(),
                        configService.getPVNameToKeyConverter());
                // Make sure that the first and last event in the file as obtained from PBFileInfo fit into the times as
                // determined from the name
                Assertions.assertTrue(
                        (fileInfo.getFirstEventEpochSeconds() >= chunkTimes.chunkStartEpochSeconds),
                        "Start time as determined by PBFileinfo "
                                + TimeUtils.convertToHumanReadableString(fileInfo.getFirstEventEpochSeconds())
                                + "is earlier than earliest time as determined by partition name"
                                + TimeUtils.convertToHumanReadableString(chunkTimes.chunkStartEpochSeconds));
                Assertions.assertTrue(
                        (fileInfo.getLastEventEpochSeconds() <= chunkTimes.chunkEndEpochSeconds),
                        "End time as determined by PBFileinfo "
                                + TimeUtils.convertToHumanReadableString(fileInfo.getLastEventEpochSeconds())
                                + "is later than latest time as determined by partition name"
                                + TimeUtils.convertToHumanReadableString(chunkTimes.chunkEndEpochSeconds));
            }
            srcSetup.deleteTestFolder();
        }
    }
}