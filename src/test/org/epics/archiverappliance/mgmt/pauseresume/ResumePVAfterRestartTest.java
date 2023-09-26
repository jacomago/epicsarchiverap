package org.epics.archiverappliance.mgmt.pauseresume;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Create a paused PV in persistence; start the appserver and make sure we can resume
 * @author mshankar
 *
 */
@Tag("integration")@Tag("localEpics")
public class ResumePVAfterRestartTest {
	private static Logger logger = LogManager.getLogger(ResumePVAfterRestartTest.class.getName());
	private File persistenceFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "DeletePVTest");
	private String pvNameToArchive = "UnitTestNoNamingConvention:sine";
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@BeforeEach
	public void setUp() throws Exception {
		if(persistenceFolder.exists()) {
			FileUtils.deleteDirectory(persistenceFolder);
		}
		persistenceFolder.mkdirs();
		System.getProperties().put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.JDBM2Persistence");
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig_appliance0.jdbm2");
		JDBM2Persistence persistenceLayer = new JDBM2Persistence();
		persistenceLayer.putTypeInfo(pvNameToArchive, generatePVTypeInfo(pvNameToArchive, "appliance0"));

		siocSetup.startSIOCWithDefaultDB();
		// Replace the testconfig_appliance0.jdbm2 with testconfig.jdbm2 as TomcatSetup adds this to the JDBM2 file name to make the tests work in a cluster
		System.getProperties().put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFolder.getPath() + File.separator + "testconfig.jdbm2");
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
	}

	@AfterEach
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();
	}

	@Test
	public void testResumePVAfterRestart() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 {
			 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
			 pvstextarea.sendKeys(pvNameToArchive);
			 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
			 checkStatusButton.click();
			 Thread.sleep(2*1000);
			 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
			 String pvNameObtainedFromTable = statusPVName.getText();
			 Assertions.assertTrue(pvNameToArchive.equals(pvNameObtainedFromTable), "PV Name is not " + pvNameToArchive + "; instead we get " + pvNameObtainedFromTable);
			 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
			 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
			 String expectedPVStatus = "Paused";
			 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
		 }

		 logger.info("Let's go to the details page and resume the PV");
		 driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
		 { 
			 Thread.sleep(2*1000);
			 WebElement resumeArchivingButn = driver.findElement(By.id("pvDetailsResumeArchiving"));
			 logger.info("Clicking on the button to resume archiving the PV");
			 resumeArchivingButn.click();
		 }
		 Thread.sleep(30*1000);
		 driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
		 {
			 Thread.sleep(2*1000);
			 WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
			 List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
			 boolean foundConnectedStatus = false;
			 for(WebElement pvDetailsTableRow : pvDetailsTableRows) {
				 WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
				 String firstCol = pvDetailsTableFirstCol.getText();
				 if(firstCol.contains("Is this PV paused:")) {
					 WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
					 String obtainedPauseStatus = pvDetailsTableSecondCol.getText();
					 String expectedPauseStatus = "No";
					 Assertions.assertTrue(expectedPauseStatus.equals(obtainedPauseStatus), "Expecting paused status to be " + expectedPauseStatus + "; instead it is " + obtainedPauseStatus);
				 } else if(firstCol.contains("Is this PV currently connected?")) {
					 WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
					 String obtainedConnectedStatus = pvDetailsTableSecondCol.getText();
					 String expectedConnectedStatus = "yes";
					 Assertions.assertTrue(expectedConnectedStatus.equals(obtainedConnectedStatus), "Expecting connected status to be " + expectedConnectedStatus + "; instead it is " + obtainedConnectedStatus);
					 foundConnectedStatus = true;
				 }
			 }
			 Thread.sleep(30*1000);
			 Assertions.assertTrue(foundConnectedStatus, "We are not able to find a connected status string in the PV details. This means the channel has not been started up in the engine");
		 }
	}
	
	
	private static PVTypeInfo generatePVTypeInfo(String pvName, String applianceIdentity) { 
		PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, false, 1);
		typeInfo.setUpperDisplayLimit(Double.valueOf(1.0));
		typeInfo.setLowerDisplayLimit(Double.valueOf(-1.0));
		typeInfo.setHasReducedDataSet(true);
		typeInfo.setComputedEventRate(1.0f);
		typeInfo.setComputedStorageRate(12.0f);
		typeInfo.setUserSpecifiedEventRate(1.0f);
		typeInfo.setApplianceIdentity(applianceIdentity);
		typeInfo.addArchiveField("HIHI");
		typeInfo.addArchiveField("LOLO");
		typeInfo.setSamplingPeriod(1.0f);
		typeInfo.setSamplingMethod(SamplingMethod.MONITOR);
		typeInfo.setPaused(true);
		typeInfo.setDataStores(new String[] { 
				"pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR",
				"pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY",
				"pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR"
		});
		return typeInfo;
	}
}
