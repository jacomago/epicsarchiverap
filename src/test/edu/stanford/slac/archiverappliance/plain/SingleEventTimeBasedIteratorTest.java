package edu.stanford.slac.archiverappliance.plain;

import org.apache.commons.io.FileUtils;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;

/**
 * Bug where we could not get data for 015-PSD1:VoltRef.
 * This had one event in the STS/MTS which was between the starttime and the end time.
 * The FileBackedPBEventStreamTimeBasedIterator should return a one event stream at least in this case.
 * @author mshankar
 *
 */
public class SingleEventTimeBasedIteratorTest {
    static String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "SingleEventTimeBasedIteratorTest";
    static ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "SingleEventTimeBasedIteratorTest/";
    File rootFolder = new File(rootFolderName);
    private ConfigService configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @ParameterizedTest
    @EnumSource(FileExtension.class)
    public void testSingleEvent(FileExtension fileExtension) throws Exception {
        PlainStoragePlugin pbplugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                fileExtension.getSuffix() + "://localhost?name=STS&rootFolder=" + rootFolderName
                        + "&partitionGranularity=PARTITION_HOUR",
                configService);

        File rootFolder = new File(pbplugin.getRootFolder());
        if (rootFolder.exists()) {
            FileUtils.deleteDirectory(rootFolder);
        }

        // Generate one event on Feb 21 in the current year.
        try (BasicContext context = new BasicContext()) {
            ArrayListEventStream testData = new ArrayListEventStream(
                    PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(),
                    new RemotableEventStreamDesc(type, pvName, (short) 2013));
            YearSecondTimestamp eventTs = TimeUtils.convertToYearSecondTimestamp(
                    TimeUtils.convertFromISO8601String("2013-02-21T18:45:08.570Z"));
            testData.add(new SimulationEvent(eventTs, type, new ScalarValue<Double>(6.855870246887207)));
            pbplugin.appendData(context, pvName, testData);
        }

        try (BasicContext context = new BasicContext()) {
            Path[] paths = PathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    rootFolderName,
                    pvName,
                    fileExtension.getExtensionString(),
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
            Assertions.assertEquals(1, paths.length, "We should get only one file, instead we got " + paths.length);
            long eventCount = 0;
            try (EventStream strm = FileStreamCreator.getTimeStream(
                    fileExtension,
                    pvName,
                    paths[0],
                    type,
                    TimeUtils.convertFromISO8601String("2013-02-19T10:45:08.570Z"),
                    TimeUtils.convertFromISO8601String("2013-02-22T10:45:08.570Z"),
                    false)) {
                for (@SuppressWarnings("unused") Event event : strm) {
                    eventCount++;
                }
            }
            Assertions.assertEquals(1, eventCount, "We should get at least one event; instead we got " + eventCount);
        }

        if (rootFolder.exists()) {
            FileUtils.deleteDirectory(rootFolder);
        }
    }
}