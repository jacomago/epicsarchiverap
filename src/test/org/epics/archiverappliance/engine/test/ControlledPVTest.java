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
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ConcurrentHashMap;

/**
 * test of conditional archiving
 *
 * @author Luofeng Li
 */
@Category(LocalEpicsTests.class)
public class ControlledPVTest {
	private static final Logger logger = LogManager.getLogger(ControlledPVTest.class.getName());
    private SIOCSetup ioc = null;
    private ConfigServiceForTests testConfigService;
    private final TestWriter writer = new TestWriter();

    private final String pvPrefix = ControlledPVTest.class.getSimpleName();
    @Before
    public void setUp() throws Exception {
        ioc = new SIOCSetup(pvPrefix);
        ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests(-1);
        Thread.sleep(10000);
    }

    @After
    public void tearDown() throws Exception {
        testConfigService.shutdownNow();
        ioc.stopSIOC();
    }


    /**
     * test of creating channels for 1000 pvs , controlled by one pv, to start archiving or stop
     */

    @Test
    public void controlledPV1000pvs() {


        try {

            SIOCSetup.caput(pvPrefix + "test:enable0", 0);
            Thread.sleep(2000);
            SIOCSetup.caput(pvPrefix + "test:enable1", 1);
            Thread.sleep(2000);
            for (int i = 0; i < 1000; i++) {

                String pvnameenable = "";
                if (i < 600) {
                    pvnameenable = pvPrefix + "test:enable0";
                } else {
                    pvnameenable = pvPrefix + "test:enable1";
                }

                String pvName = pvPrefix + "test_" + i;
                ArchiveEngine.archivePV(pvName, 2,
                        SamplingMethod.SCAN,
                        writer,
                        testConfigService,
                        ArchDBRTypes.DBR_SCALAR_DOUBLE,
                        null, pvnameenable, false, false);
                testConfigService.updateTypeInfoForPV(pvName, new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1));

            }

            Thread.sleep(5000);
            ConcurrentHashMap<String, ArchiveChannel> channelList = testConfigService.getEngineContext().getChannelList();
            int enablePVs = 0;
            for (String pvName : channelList.keySet()) {
                ArchiveChannel tempChannel = channelList.get(pvName);
                if (tempChannel.isEnabled()) enablePVs++;
            }

            Assert.assertEquals("400 channels should be started ,but only " + enablePVs + " were started", 400, enablePVs);
            Assert.assertFalse("test_0 should be unconnected but it is connected", channelList.get(pvPrefix + "test_0").getPVMetrics().isConnected());
            Assert.assertFalse("test_500 should be unconnected but it is connected", channelList.get(pvPrefix + "test_500").getPVMetrics().isConnected());
            Assert.assertTrue("test_700 should be connected but it is unconnected", channelList.get(pvPrefix + "test_700").getPVMetrics().isConnected());
            Assert.assertTrue("test_900 should be connected but it is unconnected", channelList.get(pvPrefix + "test_900").getPVMetrics().isConnected());
            SIOCSetup.caput(pvPrefix + "test:enable0", 1);
            Thread.sleep(2000);
            int enablePVs2 = 0;
            for (String pvName : channelList.keySet()) {
                ArchiveChannel tempChannel = channelList.get(pvName);
                if (tempChannel.isEnabled()) enablePVs2++;
            }

            Assert.assertEquals("1000 channels should be started ,but only " + enablePVs2 + " were started", 1000, enablePVs2);
            Assert.assertTrue("test_0 should be connected but it is unconnected", channelList.get(pvPrefix + "test_0").getPVMetrics().isConnected());
            Assert.assertTrue("test_500 should be connected but it is unconnected", channelList.get(pvPrefix + "test_500").getPVMetrics().isConnected());
            Assert.assertTrue("test_700 should be connected but it is unconnected", channelList.get(pvPrefix + "test_700").getPVMetrics().isConnected());
            Assert.assertTrue("test_900 should be connected but it is unconnected", channelList.get(pvPrefix + "test_900").getPVMetrics().isConnected());


            SIOCSetup.caput(pvPrefix + "test:enable1", 0);
            Thread.sleep(2000);
            int disablePVs3 = 0;
            for (String pvName : channelList.keySet()) {
                ArchiveChannel tempChannel = channelList.get(pvName);
                if (!tempChannel.isEnabled()) disablePVs3++;
            }

            Assert.assertEquals("400 channels should be stopped ,but only " + disablePVs3 + " were stopeed", 400, disablePVs3);
            Assert.assertTrue("test_0 should be connected but it is unconnected", channelList.get(pvPrefix + "test_0").getPVMetrics().isConnected());
            Assert.assertTrue("test_500 should be connected but it is unconnected", channelList.get(pvPrefix + "test_500").getPVMetrics().isConnected());
            Assert.assertFalse("test_700 should be unconnected but it is connected", channelList.get(pvPrefix + "test_700").getPVMetrics().isConnected());
            Assert.assertFalse("test_900 should be unconnected but it is connected", channelList.get(pvPrefix + "test_900").getPVMetrics().isConnected());


            SIOCSetup.caput(pvPrefix + "test:enable0", 0);
            Thread.sleep(2000);
            int disablePVs4 = 0;
            for (String pvName : channelList.keySet()) {
                ArchiveChannel tempChannel = channelList.get(pvName);
                if (!tempChannel.isEnabled()) disablePVs4++;
            }

            Assert.assertEquals("1000 channels should be stopped ,but only " + disablePVs4 + " were stopeed", 1000, disablePVs4);
            Assert.assertFalse("test_0 should be unconnected but it is connected", channelList.get(pvPrefix + "test_0").getPVMetrics().isConnected());
            Assert.assertFalse("test_500 should be unconnected but it is connected", channelList.get(pvPrefix + "test_500").getPVMetrics().isConnected());
            Assert.assertFalse("test_700 should be unconnected but it is connected", channelList.get(pvPrefix + "test_700").getPVMetrics().isConnected());
            Assert.assertFalse("test_900 should be unconnected but it is connected", channelList.get(pvPrefix + "test_900").getPVMetrics().isConnected());


        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }


}