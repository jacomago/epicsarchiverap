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
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * test of destroying channels
 * @author Luofeng Li
 *
 */
@Category(LocalEpicsTests.class)
public class ChannelDestoryTest {
    private static final Logger logger = LogManager.getLogger(ChannelDestoryTest.class.getName());
    private final TestWriter writer = new TestWriter();
    private final String pvPrefix = ChannelDestoryTest.class.getSimpleName().substring(0, 10);
    private SIOCSetup ioc = null;
    private ConfigServiceForTests testConfigService;

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
     * test of destroying the channel of the pv in scan mode
     */
    @Test
    public void scanChannelDestroy() {
        String pvName = pvPrefix + "test_0";
        try {
            ArchiveEngine.archivePV(
                    pvName,
                    2,
                    SamplingMethod.SCAN,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);
            Thread.sleep(2000);

            ArchiveEngine.destoryPv(pvName, testConfigService);
            Thread.sleep(2000);
            ArchiveChannel archiveChannel =
                    testConfigService.getEngineContext().getChannelList().get(pvName);
            Assert.assertNull("the channel for " + pvName + " should be destroyed but it is not", archiveChannel);

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * the test of destroying the channel of the pv in monitor mode
     */
    @Test
    public void monitorChannelDestroy() {
        String pvName = pvPrefix + "test_1";
        try {

            ArchiveEngine.archivePV(
                    pvName,
                    0.1F,
                    SamplingMethod.MONITOR,
                    writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null,
                    false,
                    false);
            Thread.sleep(2000);

            ArchiveEngine.destoryPv(pvName, testConfigService);
            Thread.sleep(2000);
            ArchiveChannel archiveChannel =
                    testConfigService.getEngineContext().getChannelList().get(pvName);
            Assert.assertNull("the channel for " + pvName + " should be destroyed but it is not", archiveChannel);

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }
}
