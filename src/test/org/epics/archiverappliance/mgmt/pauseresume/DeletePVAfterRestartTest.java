package org.epics.archiverappliance.mgmt.pauseresume;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * Create a paused PV in persistence; start the appserver and make sure we can delete
 *
 * @author mshankar
 */
@Category(IntegrationTests.class)
public class DeletePVAfterRestartTest {
    private static final Logger logger = LogManager.getLogger(DeletePVAfterRestartTest.class.getName());
    private final File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DeletePVTest");

    private final String pvPrefix = DeletePVAfterRestartTest.class.getSimpleName();
    private final String pvNameToArchive = pvPrefix + "UnitTestNoNamingConvention:sine";
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup(pvPrefix);
    WebDriver driver;

    @BeforeClass
    public static void setupClass() {
        WebDriverManager.firefoxdriver().setup();
    }

    private static PVTypeInfo generatePVTypeInfo(String pvName) {
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, false, 1);
        typeInfo.setUpperDisplayLimit(1.0);
        typeInfo.setLowerDisplayLimit(-1.0);
        typeInfo.setHasReducedDataSet(true);
        typeInfo.setComputedEventRate(1.0f);
        typeInfo.setComputedStorageRate(12.0f);
        typeInfo.setUserSpecifiedEventRate(1.0f);
        typeInfo.setApplianceIdentity("appliance0");
        typeInfo.addArchiveField("HIHI");
        typeInfo.addArchiveField("LOLO");
        typeInfo.setSamplingPeriod(1.0f);
        typeInfo.setSamplingMethod(SamplingMethod.MONITOR);
        typeInfo.setPaused(true);
        typeInfo.setDataStores(new String[]{
                "pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR",
                "pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY",
                "pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR"
        });
        return typeInfo;
    }

    @Before
    public void setUp() throws Exception {
        if (persistenceFolder.exists()) {
            FileUtils.deleteDirectory(persistenceFolder);
        }
        persistenceFolder.mkdirs();
        System.getProperties().put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
        System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig_appliance0.jdbm2");
        JDBM2Persistence persistenceLayer = new JDBM2Persistence();
        persistenceLayer.putTypeInfo(pvNameToArchive, generatePVTypeInfo(pvNameToArchive));

		siocSetup.startSIOCWithDefaultDB();
		// Replace the testconfig_appliance0.jdbm2 with testconfig.jdbm2 as TomcatSetup adds this to the JDBM2 file name to make the tests work in a cluster
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
	}

    @After
    public void tearDown() throws Exception {
        driver.quit();
        siocSetup.stopSIOC();
    }

    @Test
    public void testSimpleDeletePV() throws Exception {
        driver.get("http://localhost:17665/mgmt/ui/index.html");
        WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
        pvstextarea.sendKeys(pvNameToArchive);
        WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
        checkStatusButton.click();
        Thread.sleep(2 * 1000);
        WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
        String pvNameObtainedFromTable = statusPVName.getText();
        assertEquals("PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable, pvNameToArchive, pvNameObtainedFromTable);
        WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
        String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
        String expectedPVStatus = "Paused";
	    assertEquals("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus, pvArchiveStatusObtainedFromTable);

        logger.info("Let's go to the details page and resume the PV");
        driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
        {
            Thread.sleep(2 * 1000);
            WebElement deletePVButn = driver.findElement(By.id("pvDetailsStopArchiving"));
            logger.info("Clicking on the button to delete/stop archiving the PV");
            deletePVButn.click();
            Thread.sleep(2 * 1000);
            WebElement dialogOkButton = driver.findElement(By.id("pvStopArchivingOk"));
            logger.info("About to submit");
            dialogOkButton.click();
            Thread.sleep(10 * 1000);
        }
        {
            driver.get("http://localhost:17665/mgmt/ui/index.html");
            checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
            checkStatusButton.click();
            Thread.sleep(2 * 1000);
            statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
            pvNameObtainedFromTable = statusPVName.getText();
	        assertEquals("PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable, pvNameToArchive, pvNameObtainedFromTable);
            statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
            pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
            expectedPVStatus = "Not being archived";
	        assertEquals("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus, pvArchiveStatusObtainedFromTable);
        }
    }
}
