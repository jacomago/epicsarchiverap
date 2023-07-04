package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.ParallelEpicsIntegrationTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetArchivedPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetPVStatus;
import org.epics.nt.NTTable;
import org.epics.pvaccess.client.rpc.RPCClient;
import org.epics.pvaccess.client.rpc.RPCClientFactory;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.ScalarType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * {@link PvaGetArchivedPVs}
 * 
 * @author Kunal Shroff
 *
 */
@Category(ParallelEpicsIntegrationTests.class)
public class PvaGetPVStatusTest {

	private static Logger logger = LogManager.getLogger(PvaGetPVStatusTest.class.getName());
	private static final String pvPrefix = PvaGetPVStatusTest.class.getSimpleName().substring(0, 10);

	static TomcatSetup tomcatSetup = new TomcatSetup();
	static SIOCSetup siocSetup = new SIOCSetup();

	private static RPCClient client;

	@BeforeClass
	public static void setup() {
		logger.info("Set up for the PvaGetArchivedPVsTest");
		try {
			siocSetup.startSIOCWithDefaultDB();
			tomcatSetup.setUpDefaultWebApp();

			Thread.sleep(3*60*1000);
		
			logger.info(ZonedDateTime.now(ZoneId.systemDefault())
					+ " Waiting three mins for the service setup to complete");
			client = RPCClientFactory.create(PVA_MGMT_SERVICE);
		} catch (Exception e) {
			logger.log(Level.FATAL, e.getMessage(), e);
		}
	}

	@AfterClass
	public static void tearDown() {
		logger.info("Tear Down for the PvaGetArchivedPVsTest");
		try {
			client.destroy();
			siocSetup.stopSIOC();
		} catch (Exception e) {
			logger.log(Level.FATAL, e.getMessage(), e);
		}
	}

	@Test
	public void archivedPVTest() {
		List<String> pvNamesAll = new ArrayList<String>(1000);
		List<String> pvNamesEven = new ArrayList<String>(500);
		List<String> pvNamesOdd = new ArrayList<String>(500);
		List<String> expectedStatus = new ArrayList<String>(1000);
		for (int i = 0; i < 1000; i++) {
			pvNamesAll.add(pvPrefix + "test_" + i);
			if (i % 2 == 0) {
				pvNamesEven.add(pvPrefix + "test_" + i);
				expectedStatus.add("Archived");
			} else {
				pvNamesOdd.add(pvPrefix + "test_" + i);
				expectedStatus.add("Not Archived");
			}
		}

		try {
			// Submit all the even named pv's to be archived
			NTTable archivePvStatusReqTable = NTTable.createBuilder().addDescriptor().addColumn("pv", ScalarType.pvString).create();
			archivePvStatusReqTable.getDescriptor().put(PvaArchivePVAction.NAME);
			archivePvStatusReqTable.getColumn(PVStringArray.class, "pv").put(0, pvNamesEven.size(), pvNamesEven.toArray(new String[pvNamesEven.size()]), 0);
			client.request(archivePvStatusReqTable.getPVStructure(), 30);
			
			Thread.sleep(2*60*1000);
			
			// Wait 2 mins for the pv's to start archiving
			archivePvStatusReqTable = NTTable.createBuilder().addDescriptor().addColumn("pv", ScalarType.pvString).create();
			archivePvStatusReqTable.getDescriptor().put(PvaGetPVStatus.NAME);
			archivePvStatusReqTable.getColumn(PVStringArray.class, "pv").put(0, pvNamesAll.size(), pvNamesAll.toArray(new String[pvNamesAll.size()]), 0);
			client.request(archivePvStatusReqTable.getPVStructure(), 30);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
