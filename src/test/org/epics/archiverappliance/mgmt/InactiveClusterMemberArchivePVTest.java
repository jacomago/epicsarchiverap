package org.epics.archiverappliance.mgmt;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.PVStatus;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.epics.archiverappliance.ArchiverTestClient.ArchiveEndPoint;
import static org.epics.archiverappliance.ArchiverTestClient.getCurrentStatus;
import static org.epics.archiverappliance.ArchiverTestClient.requestAction;

/**
 * Complex unit test for testing what happens when we ask to archive a PV when the member that is currently archiving it is inactive.
 * <ul>
 * <li>Create a dummy config persistence file that has PVs assigned to two instances</li>
 * <li>Start up a tomcat using this file as persistence as one of the two instances</li>
 * <li>Ask to archive PVs that belong to both instances</li>
 * <li>We should expect an already being archived message whether the PV is being archived by the active cluster member or not.</li>
 * <li>Ask to archive a PV that belongs to neither instance.</li>
 * <li>We should expect a Initial Sampling message.</li>
 * </ul>
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class InactiveClusterMemberArchivePVTest {
    private static final Logger logger = LogManager.getLogger(InactiveClusterMemberArchivePVTest.class.getName());
    File persistenceFolder = new File(
            ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "InactiveClusterMemberArchivePVTest");
    private String pvNameToArchive1 = "UnitTestNoNamingConvention:inactive1";
    private String pvNameToArchive2 = "UnitTestNoNamingConvention:inactive2";
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    private static PVTypeInfo generatePVTypeInfo(String pvName, String applianceIdentity) {
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, false, 1);
        typeInfo.setUpperDisplayLimit(1.0);
        typeInfo.setLowerDisplayLimit(-1.0);
        typeInfo.setHasReducedDataSet(true);
        typeInfo.setComputedEventRate(1.0f);
        typeInfo.setComputedStorageRate(12.0f);
        typeInfo.setUserSpecifiedEventRate(1.0f);
        typeInfo.setApplianceIdentity(applianceIdentity);
        typeInfo.addArchiveField("HIHI");
        typeInfo.addArchiveField("LOLO");
        typeInfo.setPaused(true);
        return typeInfo;
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();

        FileUtils.deleteDirectory(persistenceFolder);
    }

    @Test
    public void testRequestForArchivingThatAlreadyExistsOnInactiveMember() throws Exception {
        checkPVStatus(pvNameToArchive1, PVStatus.PAUSED);
        checkPVStatus(pvNameToArchive2, PVStatus.APPLIANCE_DOWN);
        archivePV(pvNameToArchive1, PVStatus.PAUSED);
        archivePV(pvNameToArchive2, PVStatus.APPLIANCE_DOWN);
    }

    @BeforeEach
    public void setUp() throws Exception {
        if (persistenceFolder.exists()) {
            FileUtils.deleteDirectory(persistenceFolder);
        }
        persistenceFolder.mkdirs();
        System.getProperties()
                .put(
                        ConfigService.ARCHAPPL_PERSISTENCE_LAYER,
                        "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
        System.getProperties()
                .put(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFolder.getPath() + File.separator + "testconfig_appliance0.jdbm2");
        JDBM2Persistence persistenceLayer = new JDBM2Persistence();
        persistenceLayer.putTypeInfo(pvNameToArchive1, generatePVTypeInfo(pvNameToArchive1, "appliance0"));
        persistenceLayer.putTypeInfo(pvNameToArchive2, generatePVTypeInfo(pvNameToArchive2, "appliance1"));

        siocSetup.startSIOCWithDefaultDB();
        // Replace the testconfig_appliance0.jdbm2 with testconfig.jdbm2 as TomcatSetup adds this to the JDBM2 file name
        // to make the tests work in a cluster
        System.getProperties()
                .put(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
        tomcatSetup.setUpWebApps(InactiveClusterMemberArchivePVTest.class.getSimpleName());
    }

    private void checkPVStatus(String pvName, PVStatus expectedPVStatus) throws Exception {
        List<PVStatus> statuses = getCurrentStatus(List.of(pvName));
        Assertions.assertEquals(statuses, List.of(expectedPVStatus));
    }

    private void archivePV(String pvName, PVStatus expectedPVStatus) throws Exception {
        requestAction(List.of(pvName), ArchiveEndPoint, expectedPVStatus);
    }
}
