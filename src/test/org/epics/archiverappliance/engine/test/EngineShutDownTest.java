/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.Iterator;

import junit.framework.TestCase;
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

import java.io.File;
import java.util.Iterator;

/**
 * test of engine shuting down
 *
 * @author Luofeng Li
 */
@Category(ParallelEpicsIntegrationTests.class)
public class EngineShutDownTest extends TestCase {
	private static final Logger logger = LogManager.getLogger(EngineShutDownTest.class.getName());
    private final String pvPrefix = EngineShutDownTest.class.getSimpleName().substring(0, 10);
    private SIOCSetup ioc = null;
    private ConfigServiceForTests testConfigService;
    private final WriterTest writer = new WriterTest();

    @Before
    public void setUp() throws Exception {
        ioc = new SIOCSetup(pvPrefix);
        ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests();
        Thread.sleep(3000);
    }

    @After
    public void tearDown() throws Exception {


        ioc.stopSIOC();

    }

    @Test
    public void testAll() {
        engineShutDown();
    }

    /**
     * test of engine shutting down
     */
    private void engineShutDown() {

        try {
            for (int m = 0; m < 100; m++) {
                ArchiveEngine.archivePV(pvPrefix + "test_" + m, 0.1F, SamplingMethod.SCAN,
                        5, writer, testConfigService,
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
                Thread.sleep(10);
            }
            Thread.sleep(2000);

            testConfigService.shutdownNow();
            Thread.sleep(2000);
            int num = 0;
            Iterator<String> allpvs = testConfigService
                    .getPVsForThisAppliance().iterator();
            while (allpvs.hasNext()) {
                allpvs.next();
                num++;
            }


	        assertEquals("there should be no pvs after the engine shut down, but there are "
			        + num + " pvs", 0, num);

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }

    }

}
