package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.epics.archiverappliance.ArchiverTestClient.archivePV;
import static org.epics.archiverappliance.ArchiverTestClient.deletePV;
import static org.epics.archiverappliance.retrieval.client.RetreivalTestUtil.checkRetrieval;

/**
 * Generate known amount of data for a PV; corrupt known number of the values.
 * Retrieve data using mean_600 and raw and make sure we do not drop the stream entirely.
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class PostProcessorWithPBErrorTest {
    private static final Logger logger = LogManager.getLogger(PostProcessorWithPBErrorTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    private static final String pvPrefix = PostProcessorWithPBErrorDailyTest.class.getSimpleName();

    private final String pvName = pvPrefix + "UnitTestNoNamingConvention:inactive1";
    private final short currentYear = TimeUtils.getCurrentYear();
    private final String ltsFolderName = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");
    private final File ltsFolder = new File(ltsFolderName + "/UnitTestNoNamingConvention");
    StoragePlugin storageplugin;
    private ConfigServiceForTests configService;
    private final short dataGeneratedForYears = 5;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        storageplugin = StoragePluginURLParser.parseStoragePlugin(
                "pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR",
                configService);
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        if (ltsFolder.exists()) {
            FileUtils.deleteDirectory(ltsFolder);
        }
        try (BasicContext context = new BasicContext()) {
            for (short y = dataGeneratedForYears; y > 0; y--) {
                short year = (short) (currentYear - y);
                for (int day = 0; day < 365; day++) {
                    ArrayListEventStream testData = new ArrayListEventStream(
                            24 * 60 * 60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
                    int startofdayinseconds = day * 24 * 60 * 60;
                    for (int secondintoday = 0; secondintoday < 24 * 60 * 60; secondintoday += 60) {
                        // The value should be the secondsIntoYear integer divided by 600.
                        testData.add(new SimulationEvent(
                                startofdayinseconds + secondintoday,
                                year,
                                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                                new ScalarValue<Double>(
                                        (double) (((int) (startofdayinseconds + secondintoday) / 600)))));
                    }
                    storageplugin.appendData(context, pvName, testData);
                }
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        deletePV(pvName);
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    void testRetrievalWithPostprocessingAndCorruption() throws Exception {
        archivePV(pvName);

        int totalCount = checkRetrieval(pvName, dataGeneratedForYears * 365 * 24 * 60, true, dataGeneratedForYears);
        logger.info("*** -> Corrupting some data now");
        corruptSomeData();

        // We have now archived this PV, get some data and validate we got the expected number of events
        // We generated data for dataGeneratedForYears years; one sample every minute
        // We should get 365*24*60 events if things were ok.
        // However, we corrupted each file; so we should lose maybe 1000 events per file?
        checkRetrieval(pvName, totalCount - dataGeneratedForYears * 1000, false, dataGeneratedForYears);
        checkRetrieval(
                "mean_600(" + pvName + ")",
                totalCount / 10 - dataGeneratedForYears * 100,
                false,
                dataGeneratedForYears);
        checkRetrieval(
                "firstSample_600(" + pvName + ")",
                totalCount / 10 - dataGeneratedForYears * 100,
                false,
                dataGeneratedForYears);
        checkRetrieval(
                "lastSample_600(" + pvName + ")",
                totalCount / 10 - dataGeneratedForYears * 100,
                false,
                dataGeneratedForYears);
    }

    private void corruptSomeData() throws Exception {
        try (BasicContext context = new BasicContext()) {
            Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(
                    context.getPaths(),
                    ltsFolderName,
                    pvName,
                    PlainPBStoragePlugin.pbFileExtension,
                    PartitionGranularity.PARTITION_YEAR,
                    CompressionMode.NONE,
                    configService.getPVNameToKeyConverter());
            Assertions.assertTrue(true);
            Assertions.assertTrue(paths.length > 0);
            // Corrupt each file
            for (Path path : paths) {
                try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
                    Random random = new Random();
                    // Seek to a well defined spot.
                    int bytesToOverwrite = 100;
                    long randomSpot = 512 + (long) ((channel.size() - 512) * 0.33);
                    channel.position(randomSpot - bytesToOverwrite);
                    ByteBuffer buf = ByteBuffer.allocate(bytesToOverwrite);
                    byte[] junk = new byte[bytesToOverwrite];
                    // Write some garbage
                    random.nextBytes(junk);
                    buf.put(junk);
                    buf.flip();
                    channel.write(buf);
                }
            }
        }
    }
}
