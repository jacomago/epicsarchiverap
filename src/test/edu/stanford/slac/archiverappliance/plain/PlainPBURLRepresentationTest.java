package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.data.PBCommonSetup;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class PlainPBURLRepresentationTest {

    @ParameterizedTest
    @EnumSource(FileExtension.class)
    public void testToAndFromURL(FileExtension fileExtension) throws Exception {
        PlainStoragePlugin etlSrc = new PlainStoragePlugin(fileExtension);
        PBCommonSetup srcSetup = new PBCommonSetup();

        srcSetup.setUpRootFolder(
                etlSrc, "SimpleETLTestSrc_" + PartitionGranularity.PARTITION_HOUR, PartitionGranularity.PARTITION_HOUR);
        String urlRep = etlSrc.getURLRepresentation();
        ConfigService configService = new ConfigServiceForTests(-1);
        PlainStoragePlugin after =
                (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(urlRep, configService);
        assert after != null;
        Assertions.assertEquals(
                after.getRootFolder(),
                etlSrc.getRootFolder(),
                "Source folders are not the same" + after.getRootFolder() + etlSrc.getRootFolder());
    }
}