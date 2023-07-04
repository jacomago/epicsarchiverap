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
import org.epics.archiverappliance.ParallelEpicsIntegrationTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

/**
 * test of sample buffer over flow
 * @author Luofeng Li
 *
 */
@Category(ParallelEpicsIntegrationTests.class)
public class SampleBufferOverFlowTest extends TestCase {
	private static Logger logger = LogManager.getLogger(SampleBufferOverFlowTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private WriterTest writer = new WriterTest();

	private final String pvPrefix = SampleBufferOverFlowTest.class.getSimpleName().substring(0, 10);
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
		sampleBufferOverflow();
	}

	private void sampleBufferOverflow() {
		String pvName = pvPrefix + "test_1000";
		try {
			ArchiveEngine.archivePV(pvName, 5F, SamplingMethod.MONITOR, 10,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null, false, false);

			Thread.sleep(30000);

			long num = ArchiveEngine.getMetricsforPV(pvName, testConfigService)
					.getSampleBufferFullLostEventCount();
			assertTrue(
					"the number of data lost because of sample buffer overflow of "
							+ pvName
							+ "is 0,and maybe "
							+ "the pv of "
							+ pvName
							+ " changes too slow and for this test,it should changes every 1 second",
					num > 0);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}

}
