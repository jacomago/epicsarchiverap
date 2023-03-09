/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test for meta data archiving
 * 
 * @author Luofeng Li
 * 
 */
@Category(LocalEpicsTests.class)
public class ArchiveFieldsTest {
	private static final Logger logger = LogManager.getLogger(ArchiveFieldsTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;

	private final String pvPrefix = ArchiveFieldsTest.class.getSimpleName();
	@Before
	public void setUp() throws Exception {
		ioc = new SIOCSetup(pvPrefix);
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests(-1);
		Thread.sleep(3000);
	}

	@After
	public void tearDown() throws Exception {

		testConfigService.shutdownNow();
		ioc.stopSIOC();

	}


	/**
	 * test one pv with meta field. We must make sure the meta fields should be
	 * archived too
	 */
	@Test
	public void oneChannelWithMetaField() {

		try {
			String pvName = pvPrefix + "test_NOADEL";
			MemBufWriter myWriter = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
			PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.addArchiveField("HIHI");
			typeInfo.addArchiveField("LOLO");
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			testConfigService.registerPVToAppliance(pvName, testConfigService.getMyApplianceInfo());

			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.MONITOR, myWriter, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, typeInfo.getArchiveFields(), false, false);
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15 * 1000);
			assertFalse("Not emough delay - metafields still need starting up", testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp());
			logger.info("Changing fields");
			SIOCSetup.caput(pvName + ".HIHI", 80);
			SIOCSetup.caput(pvName + ".LOLO", 5);
			Thread.sleep(1000);
			SIOCSetup.caput(pvName + ".HIHI", 85);
			SIOCSetup.caput(pvName + ".LOLO", 6);
			logger.info("Done changing fields");
			Thread.sleep(30000);

			int hihiNum = 0;
			int loloNUm = 0;
			int totalEvents = 0;
			for (Event e : myWriter.getCollectedSamples()) {
				DBRTimeEvent tempDBRTimeEvent = (DBRTimeEvent) e;
				String hihiVluue = tempDBRTimeEvent.getFieldValue("HIHI");
				if (hihiVluue != null) {
					hihiNum++;
				}

				String loloVluue = tempDBRTimeEvent.getFieldValue("LOLO");
				if (loloVluue != null) {
					loloNUm++;
				}
				totalEvents++;
			}
			assertTrue("We should have some events in the current samples " + totalEvents, totalEvents >= 2);
			assertTrue("the number of value for test_0.HIHI num is " + hihiNum
					+ " and <3 and" + "it should be >=3", hihiNum >= 2);
			assertTrue("the number of value for test_0.LOLO num is " + loloNUm
					+ " and <3 and" + "it should be >=3", loloNUm >= 2);
			Thread.sleep(3000);

		} catch (Exception e) {
			//
			fail(e.getMessage());
			logger.error("Exception", e);
		}
	}

	/**
	 * test one pv with meta field.this pv and the meta fields are controlled by
	 * another pv to start or stop archiving. We must make sure when the pv is
	 * stopped or started archiving ,all the meta field should be stopped or
	 * stated at the same time
	 */
	@Test
	public void oneChannelWithMetaFieldWithControlPv() {

		try {
			String pvName = pvPrefix + "test_1";

			MemBufWriter myWriter = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);

			String controlPVName = pvPrefix + "test:enable0";
			SIOCSetup.caput(controlPVName, 1);
			Thread.sleep(3000);
			String[] metaFields = { "HIHI", "LOLO" };
			PVTypeInfo typeInfo = new PVTypeInfo(pvName,ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.setSamplingMethod(SamplingMethod.SCAN);
			typeInfo.setSamplingPeriod(20);
			typeInfo.setDataStores(new String[] {"blackhole://localhost"});
			typeInfo.setArchiveFields(metaFields);
			typeInfo.setControllingPV(controlPVName);
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.SCAN, myWriter,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null,
					controlPVName, metaFields, null, false, false);
			Thread.sleep(10 * 1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(10 * 1000);
			assertFalse("Not enough delay - metafields still need starting up", testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp());
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);

			boolean samplesExist = !myWriter.getCollectedSamples().isEmpty();
			boolean result = archiveChannel.isConnected()
					&& samplesExist;
			assertTrue(pvName + " is not started successfully and it should be started successfully", result);

			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp + " should be created and connected but it is not", archiveChannel.isMetaPVConnected(metaFieldTemp));

			}
			Thread.sleep(10 * 1000);
			SIOCSetup.caput(controlPVName, 0);
			testConfigService.getEngineContext().getWriteThead().flushBuffer();
			Thread.sleep(10 * 1000);
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			assertTrue(pvName + " is not stopped successfully and it should be stopped successfully", archiveChannel == null || !archiveChannel.isConnected());
			assertTrue(pvName + "should not have any data", archiveChannel == null || archiveChannel.getSampleBuffer().getCurrentSamples().isEmpty());

			if(archiveChannel != null) { 
				for (String metaFieldTemp : metaFields) {
					String pvNameTemp = pvName + "." + metaFieldTemp;
					assertFalse("the channel for " + pvNameTemp
							+ " should be not connected but it is ", archiveChannel.isMetaPVConnected(metaFieldTemp));
				}
			}

			Thread.sleep(10 * 1000);
			SIOCSetup.caput(controlPVName, 1);
			Thread.sleep(10 * 1000);
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			assertNotNull("After resuming the control channel, the archive channel for pv " + pvName + " is still null", archiveChannel);
			boolean result3 = archiveChannel.isConnected();
			assertTrue(
					pvName
							+ " is not started successfully and it should be started successfully",
					result3);

			Thread.sleep(10 * 1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(10 * 1000);
			assertFalse("Not emough delay - metafields still need starting up", testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp());

			// check meta field is not connected
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp
						+ " should be reconnected but it is  not",
						archiveChannel.isMetaPVConnected(metaFieldTemp));

			}

		} catch (Exception e) {
			//
			fail(e.getMessage());
			logger.error("Exception", e);
		}
	}

}