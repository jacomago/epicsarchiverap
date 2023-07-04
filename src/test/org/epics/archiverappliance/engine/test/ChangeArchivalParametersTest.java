

/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import junit.framework.TestCase;
/**
 * test for changing archiving parameters of pvs.
 * @author Luofeng Li
 *
 */
@Category(LocalEpicsTests.class)
public class ChangeArchivalParametersTest extends TestCase {
	private static Logger logger = LogManager.getLogger(ChangeArchivalParametersTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private WriterTest writer = new WriterTest();

	private final String pvPrefix = ChangeArchivalParametersTest.class.getSimpleName().substring(0, 10);
	@Before
	public void setUp() throws Exception {
		ioc = new SIOCSetup(pvPrefix);
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests();
		Thread.sleep(3000);
	}

	@After
	public void tearDown() throws Exception {

		testConfigService.shutdownNow();
		ioc.stopSIOC();

	}

	@Test
	public void testAll() {
		changeArchivalParametersFromScanToScan();
		changeArchivalParametersFromScanToMonitor();
		changeArchivalParametersFromMonitorToScan();
		changeArchivalParametersFromMonitorToMonitor();

	}
/**
 * test of changing one pv from scan mode to scan mode ,but with a different sample period
 */
	private void changeArchivalParametersFromScanToScan() {

		String pvName = pvPrefix + "test_0";

		try {

			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);

			Thread.sleep(5000);

			ArchiveEngine.changeArchivalParameters(pvName, 8,
					SamplingMethod.SCAN, testConfigService, writer, false, false);

			Thread.sleep(11000);

			// ArchiveChannel
			// archiveChannel=testConfigService.getEngineContext().getChannelList().get(pvName);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			double period = tempPVMetrics.getSamplingPeriod();
			boolean isMonitor = tempPVMetrics.isMonitor();
			assertTrue(
					"the "
							+ pvName
							+ " should be archived in scan mode but it is monitor mode",
					!isMonitor);
			assertTrue("the new sample period is " + period + " that is not 8",
					(period - 8) == 0);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			int valueNumber = archiveChannel.getSampleBuffer().getCurrentSamples().size();
			assertTrue("there is no data in sample buffer", valueNumber > 0);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
/**
 * test of changing pv from scan mode to monitor mode
 */
	private void changeArchivalParametersFromScanToMonitor() {

		String pvName = pvPrefix + "test_1";
		try {

			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);

			Thread.sleep(5000);

			ArchiveEngine.changeArchivalParameters(pvName, 0.1F,
					SamplingMethod.MONITOR, testConfigService, writer, false, false);

			Thread.sleep(5000);
			// ArchiveChannel
			// archiveChannel=testConfigService.getEngineContext().getChannelList().get(pvName);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			boolean isMonitor = tempPVMetrics.isMonitor();
			assertTrue(
					"the "
							+ pvName
							+ " should be archived in monitor mode but it is scan mode",
					isMonitor);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			int valueNumber = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size();
			assertTrue("there is no data in sample buffer", valueNumber > 0);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
/**
 * test of changing pv from monitor mode to scan mode
 */
	private void changeArchivalParametersFromMonitorToScan() {

		String pvName = pvPrefix + "test_2";

		try {

			ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.MONITOR, 60,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null, false, false);

			Thread.sleep(5000);
			ArchiveEngine.changeArchivalParameters(pvName, 2,
					SamplingMethod.SCAN, testConfigService, writer, false, false);
			Thread.sleep(5000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			boolean isMonitor = tempPVMetrics.isMonitor();
			assertTrue(
					"the "
							+ pvName
							+ " should be archived in scan mode but it is monitor mode",
					!isMonitor);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			int valueNumber = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size();
			assertTrue("there is no data in sample buffer", valueNumber > 0);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
/**
 * test of  changing pv from monitor mode to monitor mode
 */
	private void changeArchivalParametersFromMonitorToMonitor() {

		String pvName = pvPrefix + "test_3";
		try {

			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.MONITOR, 60,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null, false, false);
			ArchiveEngine.changeArchivalParameters(pvName, 0.1F,
					SamplingMethod.MONITOR, testConfigService, writer, false, false);
			Thread.sleep(5000);
			ArchiveEngine.changeArchivalParameters(pvName, 2,
					SamplingMethod.MONITOR, testConfigService, writer, false, false);
			Thread.sleep(5000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			boolean isMonitor = tempPVMetrics.isMonitor();
			assertTrue(
					"the "
							+ pvName
							+ " should be archived in monitor mode but it is scan mode",
					isMonitor);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			int valueNumber = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size();
			assertTrue("there is no data in sample buffer", valueNumber > 0);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}

}
