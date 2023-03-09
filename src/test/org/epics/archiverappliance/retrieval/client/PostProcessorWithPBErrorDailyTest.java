package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.CompressionMode;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Generate known amount of data for a PV; corrupt known number of the values.
 * Retrieve data using mean_600 and raw and make sure we do not drop the stream entirely.
 * This is similar to the PostProcessorWithPBErrorTest but uses the daily partitions
 * We need to test both FileBackedPBEventStreamPositionBasedIterator and the FileBackedPBEventStreamTimeBasedIterator for handling PBExceptions.
 * The yearly partition has this effect of using only FileBackedPBEventStreamTimeBasedIterator at this point in time.
 * So this additional test.
 * @author mshankar
 *
 */
@Category({IntegrationTests.class, LocalEpicsTests.class})
public class PostProcessorWithPBErrorDailyTest {
	private static Logger logger = LogManager.getLogger(PostProcessorWithPBErrorDailyTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	SIOCSetup siocSetup = new SIOCSetup();
	WebDriver driver;
	private String pvName = "UnitTestNoNamingConvention:inactive1";
	private short currentYear = TimeUtils.getCurrentYear();
	private String mtsFolderName = System.getenv("ARCHAPPL_MEDIUM_TERM_FOLDER"); 
	private File mtsFolder = new File(mtsFolderName + "/UnitTestNoNamingConvention");
	StoragePlugin storageplugin;
	private ConfigServiceForTests configService;
	private short dataGeneratedForYears = 5;

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(-1);
		storageplugin = StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY", configService);
		siocSetup.startSIOCWithDefaultDB();
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
		
		if(mtsFolder.exists()) { 
			FileUtils.deleteDirectory(mtsFolder);
		}
		try(BasicContext context = new BasicContext()) { 
			for(short y = dataGeneratedForYears; y > 0; y--) { 
				short year = (short)(currentYear - y);
				for(int day = 0; day < 365; day++) {
					ArrayListEventStream testData = new ArrayListEventStream(24*60*60, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, year));
					int startofdayinseconds = day*24*60*60;
					for(int secondintoday = 0; secondintoday < 24*60*60; secondintoday += 60) {
						// The value should be the secondsIntoYear integer divided by 600.
						testData.add(new SimulationEvent(startofdayinseconds + secondintoday, year, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double) (((int)(startofdayinseconds + secondintoday)/600)))));
					}
					storageplugin.appendData(context, pvName, testData);
				}
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		driver.quit();
		tomcatSetup.tearDown();
		siocSetup.stopSIOC();

		if(mtsFolder.exists()) { 
			FileUtils.deleteDirectory(mtsFolder);
		}
	}

	@Test
	public void testRetrievalWithPostprocessingAndCorruption() throws Exception {
		 driver.get("http://localhost:17665/mgmt/ui/index.html");
		 WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		 pvstextarea.sendKeys(pvName);
		 WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
		 logger.debug("About to submit");
		 archiveButton.click();
		 // We have to wait for a few minutes here here as it does take a while for the workflow to complete.
		 Thread.sleep(5*60*1000);
		 WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
		 checkStatusButton.click();
		 Thread.sleep(2*1000);
		 WebElement statusPVName = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(1)"));
		 String pvNameObtainedFromTable = statusPVName.getText();
		 assertTrue("PV Name is not " + pvName + "; instead we get " + pvNameObtainedFromTable, pvName.equals(pvNameObtainedFromTable));
		 WebElement statusPVStatus = driver.findElement(By.cssSelector("#archstatsdiv_table tr:nth-child(1) td:nth-child(2)"));
		 String pvArchiveStatusObtainedFromTable = statusPVStatus.getText();
		 String expectedPVStatus = "Being archived";
		 assertTrue("Expecting PV archive status to be " + expectedPVStatus + "; instead it is " + pvArchiveStatusObtainedFromTable, expectedPVStatus.equals(pvArchiveStatusObtainedFromTable));
		 Thread.sleep(1*60*1000);
		 
		 int totalCount = checkRetrieval(pvName, dataGeneratedForYears*365*24*60, true);
		 corruptSomeData();
		 
		 // We have now archived this PV, get some data and validate we got the expected number of events
		 // We generated data for dataGeneratedForYears years; one sample every minute
		 // We should get 365*24*60 events if things were ok.
		 // However, we corrupted each file; so we should lose maybe 1000 events per file?
		 checkRetrieval(pvName, totalCount - dataGeneratedForYears*365*1000, false);
		 checkRetrieval("mean_600(" + pvName + ")", totalCount/10 - dataGeneratedForYears*365*100, false);
		 checkRetrieval("firstSample_600(" + pvName + ")", totalCount/10 - dataGeneratedForYears*365*100, false);
		 checkRetrieval("lastSample_600(" + pvName + ")", totalCount/10 - dataGeneratedForYears*365*100, false);
	}
	private int checkRetrieval(String retrievalPVName, int expectedAtLeastEvents, boolean exactMatch) throws IOException {
		long startTimeMillis = System.currentTimeMillis();
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		 Timestamp now = TimeUtils.now();
		 Timestamp start = TimeUtils.minusDays(now, (dataGeneratedForYears+1)*366);
		 Timestamp end = now;
		 int eventCount = 0;

		 final HashMap<String, String> metaFields = new HashMap<String, String>(); 
		 // Make sure we get the EGU as part of a regular VAL call.
		 try(GenMsgIterator strm = rawDataRetrieval.getDataForPV(retrievalPVName, start, end, false, null)) { 
			 PayloadInfo info = null;
			 assertTrue("We should get some data, we are getting a null stream back", strm != null); 
			 info =  strm.getPayLoadInfo();
			 assertTrue("Stream has no payload info", info != null);
			 mergeHeaders(info, metaFields);
			 strm.onInfoChange(new InfoChangeHandler() {
				 @Override
				 public void handleInfoChange(PayloadInfo info) {
					 mergeHeaders(info, metaFields);
				 }
			 });

			 long endTimeMillis =  System.currentTimeMillis();

			 
			 for(@SuppressWarnings("unused") EpicsMessage dbrevent : strm) {
				 eventCount++;
			 }
			 
			 logger.info("Retrival for " + retrievalPVName + "=" + (endTimeMillis - startTimeMillis) + "(ms)");
		 }

		 logger.info("For " + retrievalPVName + " we were expecting " + expectedAtLeastEvents + "events. We got " + eventCount);
		 assertTrue("For " + retrievalPVName + ", expecting " + expectedAtLeastEvents + "events. We got " + eventCount, eventCount >= expectedAtLeastEvents);
		 if(exactMatch) { 
			 assertTrue("For " + retrievalPVName + ", Expecting " + expectedAtLeastEvents + "events. We got " + eventCount, eventCount == expectedAtLeastEvents);
		 }
		 
		 return eventCount;
	}
	
	private static void mergeHeaders(PayloadInfo info, HashMap<String, String> headers) { 
		 int headerCount = info.getHeadersCount();
		 for(int i = 0; i < headerCount; i++) { 
			 String headerName = info.getHeaders(i).getName();
			 String headerValue = info.getHeaders(i).getVal();
			 logger.debug("Adding header " + headerName + " = " + headerValue);
			 headers.put(headerName, headerValue);
		 }
	}
	
	
	private void corruptSomeData() throws Exception { 
		try(BasicContext context = new BasicContext()) { 
			Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), mtsFolderName, pvName, PlainPBStoragePlugin.PB_EXTENSION, PartitionGranularity.PARTITION_DAY, CompressionMode.NONE, configService.getPVNameToKeyConverter());
			assertTrue(paths != null);
			assertTrue(paths.length > 0);
			// Corrupt each file
			for(Path path : paths) { 
				try(SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.WRITE)) {
					Random random = new Random();
					// Seek to a well defined spot.
					int bytesToOverwrite = 100;
					long randomSpot = 512 + (long)((channel.size()-512)*0.33);
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
		
		// Don't really want to see the client side exception here just yet
		java.util.logging.Logger.getLogger(InputStreamBackedGenMsg.class.getName()).setLevel(java.util.logging.Level.OFF);
	}
}
