package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import io.github.bonigarcia.wdm.WebDriverManager;

/**
 * Check addAlias and removeAlias functionality.
 * We test after the PV workflow is complete. 
 *  
 * @author mshankar
 *
 */
@Tag("integration")
@Tag("localEpics")
public class AddRemoveAliasTest {
	private static Logger logger = LogManager.getLogger(AddRemoveAliasTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@BeforeEach
	public void setUp() throws Exception {
		siocSetup.startSIOCWithDefaultDB();
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
	public void testSimpleArchivePV() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 String pvNameToArchive = "UnitTestNoNamingConvention:sine";
		 pvstextarea.sendKeys(pvNameToArchive);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 // In addition, we are also getting .HIHI etc the monitors for which get established many minutes after the beginning of archiving 
		 Thread.sleep(15*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
		 String pvNameObtainedFromTable = statusPVName.getText();
		 String expectedPVName = "UnitTestNoNamingConvention:sine";
		 Assertions.assertTrue(expectedPVName.equals(pvNameObtainedFromTable), "Expecting PV name to be " + expectedPVName + "; instead we get " + pvNameObtainedFromTable);
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 Assertions.assertTrue(expectedPVStatus.equals(pvArchiveStatusObtainedFromTable), "Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable);
		 
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 2.0);
		 Thread.sleep(2*1000);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 3.0);
		 Thread.sleep(2*1000);
		 SIOCSetup.caput("UnitTestNoNamingConvention:sine.HIHI", 4.0);
		 Thread.sleep(2*1000);
		 logger.info("Done updating UnitTestNoNamingConvention:sine.HIHI");
		 Thread.sleep(2*60*1000);
		 
		 // Test retrieval of data using the real name and the aliased name
		 testRetrievalCount("UnitTestNoNamingConvention:sine", true);
		 testRetrievalCount("UnitTestNoNamingConvention:arandomalias", false);
		 testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI", true);
		 testRetrievalCount("UnitTestNoNamingConvention:arandomalias.HIHI", false);
		 
		 String addAliasURL = "http://localhost:17665/mgmt/bpl/addAlias" 
		 + "?pv="+ URLEncoder.encode("UnitTestNoNamingConvention:sine", "UTF-8")
		 + "&aliasname="+ URLEncoder.encode("UnitTestNoNamingConvention:arandomalias", "UTF-8");
		 JSONObject addAliasStatus = GetUrlContent.getURLContentAsJSONObject(addAliasURL);
		 logger.debug("Add alias response " + addAliasStatus.toJSONString());
		 
		 Thread.sleep(2*1000);

		 testRetrievalCount("UnitTestNoNamingConvention:sine", true);
		 testRetrievalCount("UnitTestNoNamingConvention:arandomalias", true);
		 testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI", true);
		 testRetrievalCount("UnitTestNoNamingConvention:arandomalias.HIHI", true);
		 
		 String removeAliasURL = "http://localhost:17665/mgmt/bpl/removeAlias" 
		 + "?pv="+ URLEncoder.encode("UnitTestNoNamingConvention:sine", "UTF-8")
		 + "&aliasname="+ URLEncoder.encode("UnitTestNoNamingConvention:arandomalias", "UTF-8");
		 JSONObject removeAliasStatus = GetUrlContent.getURLContentAsJSONObject(removeAliasURL);
		 logger.debug("Remove alias response " + removeAliasStatus.toJSONString());
		 
		 Thread.sleep(2*1000);

		 testRetrievalCount("UnitTestNoNamingConvention:sine", true);
		 testRetrievalCount("UnitTestNoNamingConvention:arandomalias", false);
		 testRetrievalCount("UnitTestNoNamingConvention:sine.HIHI", true);
		 testRetrievalCount("UnitTestNoNamingConvention:arandomalias.HIHI", false);

	}

	/**
	 * Make sure we get some data when retriving under the given name
	 * @param pvName
	 * @param expectingData - true if we are expecting any data at all.
	 * @throws IOException
	 */
	private void testRetrievalCount(String pvName, boolean expectingData) throws IOException {
		 RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		 Timestamp end = TimeUtils.plusDays(TimeUtils.now(), 3);
		 Timestamp start = TimeUtils.minusDays(end, 6);
		try(EventStream stream = rawDataRetrieval.getDataForPVS(new String[] { pvName}, start, end, null)) {
			 long previousEpochSeconds = 0;
			 int eventCount = 0;

			 // We are making sure that the stream we get back has times in sequential order...
			 if(stream != null) {
				 for(Event e : stream) {
					 long actualSeconds = e.getEpochSeconds();
					 Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
					 previousEpochSeconds = actualSeconds;
					 eventCount++;
				 }
			 }

			 logger.info("Got " + eventCount + " event for pv " + pvName);
			 if(expectingData) { 
				 Assertions.assertTrue(eventCount > 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
			 } else { 
				 Assertions.assertTrue(eventCount == 0, "When asking for data using " + pvName + ", event count is 0. We got " + eventCount);
			 }
		 }
	}
}
