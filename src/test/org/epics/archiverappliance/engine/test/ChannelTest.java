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
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * test of creating channels
 *
 * @author Luofeng Li
 */
@Category(ParallelEpicsIntegrationTests.class)
public class ChannelTest {
	private static final Logger logger = LogManager.getLogger(ChannelTest.class.getName());
    private static final String pvPrefix = ChannelTest.class.getSimpleName();
    private static SIOCSetup ioc = null;
    private static ConfigServiceForTests testConfigService;
    private final WriterTest writer = new WriterTest();

    @BeforeClass
    public static void setUp()  {
        ioc = new SIOCSetup(pvPrefix);
	    try {
		    ioc.startSIOCWithDefaultDB();
		    testConfigService = new ConfigServiceForTests();
		    Thread.sleep(3000);
	    } catch (Exception e) {
		    throw new RuntimeException(e);
	    }
    }

    @AfterClass
    public static void tearDown() throws Exception {

        testConfigService.shutdownNow();
        ioc.stopSIOC();

    }

    /**
     * test of creating the channel for the pv in scan mode
     */
    @Test
    public void singleScanChannel() {

        String pvName = pvPrefix + "test_0";
        try {
            ArchiveEngine.archivePV(pvName, 1, SamplingMethod.SCAN, 60, writer,
                    testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);

            Thread.sleep(62000);
            ArchiveChannel archiveChannel = testConfigService
                    .getEngineContext().getChannelList().get(pvName);
	        assertNotNull("the channel for " + pvName
			        + " should be created but it is not", archiveChannel);
            boolean hasData = archiveChannel.getSampleBuffer()
                    .getCurrentSamples().size() > 0;
            assertTrue("the channel for " + pvName
                    + " should have data but it don't", hasData);

            ArchiveEngine.destoryPv(pvName, testConfigService);

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * test of creating the channel for the pv in monitor mode
     */
    @Test
    public void singleMonitorChannel() {
        String pvName = pvPrefix + "test_1";
        try {
            ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.MONITOR, 60,
                    writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null, false, false);
            Thread.sleep(5000);
            ArchiveChannel archiveChannel = testConfigService
                    .getEngineContext().getChannelList().get(pvName);
	        assertNotNull("the channel for " + pvName
			        + " should be created but it is not", archiveChannel);
            boolean hasData = archiveChannel.getSampleBuffer()
                    .getCurrentSamples().size() > 0;
            assertTrue("the channel for " + pvName
                    + " should have data but it don't", hasData);

            ArchiveEngine.destoryPv(pvName, testConfigService);
        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * test of starting or stopping archiving one pv
     */
    @Test
    public void stopAndRestartChannel() {

        String pvName = pvPrefix + ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":test_2";
        try {

            PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
            typeInfo.setSamplingMethod(SamplingMethod.SCAN);
            typeInfo.setSamplingPeriod(1);
            typeInfo.setDataStores(new String[]{"blackhole://localhost"});
            testConfigService.updateTypeInfoForPV(pvName, typeInfo);

            ArchiveEngine.archivePV(pvName, 1, SamplingMethod.SCAN, 60, writer,
                    testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
            Thread.sleep(2000);
            ArchiveChannel archiveChannel = testConfigService
                    .getEngineContext().getChannelList().get(pvName);
	        assertNotNull("the channel for " + pvName
			        + " should be created but it is not", archiveChannel);
            boolean hasData = archiveChannel.getSampleBuffer()
                    .getCurrentSamples().size() > 0;
            assertTrue("the channel for " + pvName
                    + " should have data but it don't", hasData);
            ArchiveEngine.pauseArchivingPV(pvName, testConfigService);
            Thread.sleep(2000);
            archiveChannel.getSampleBuffer().getCurrentSamples().clear();
            Thread.sleep(2000);
            PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
            assertTrue("the channel for " + pvName
                            + " should be stopped but it is not",
                    tempPVMetrics == null || !tempPVMetrics.isConnected());
            boolean hasData2 = archiveChannel.getSampleBuffer()
                    .getCurrentSamples().size() > 0;
	        assertFalse("the channel for " + pvName
			        + " should not have data but it has", hasData2);

            ArchiveEngine.resumeArchivingPV(pvName, testConfigService);
            Thread.sleep(12000);
            PVMetrics tempPVMetrics3 = ArchiveEngine.getMetricsforPV(pvName,
                    testConfigService);
            assertTrue("the channel for " + pvName
                            + " should be restarted but it is not",
                    tempPVMetrics3.isConnected());
            archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
            boolean hasData3 = archiveChannel.getSampleBuffer()
                    .getCurrentSamples().size() > 0;
            assertTrue("the channel for " + pvName
                    + " should have data but it don't", hasData3);

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }

    }

    /**
     * test of creating channels for 1000 pvs in scan mode
     */
    @Test
    public void create1000ScanChannel() {
        try {

            for (int m = 0; m < 1000; m++) {
                String pvName = pvPrefix + "test_" + m;
                ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.SCAN, 60,
                        writer, testConfigService,
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
                Thread.sleep(10);
            }
            Thread.sleep(2000);
            int num = 0;
            for (int m = 0; m < 1000; m++) {
                String pvName = pvPrefix + "test_" + m;
                PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
                        testConfigService);
                if (tempPVMetrics.isConnected())
                    num++;

            }
	        assertEquals("Only "
			        + num
			        + " of 1000 of channels in scan mode connected successfully", 1000, num);
            for (int m = 0; m < 1000; m++) {
                String pvName = pvPrefix + "test_" + m;
                ArchiveEngine.destoryPv(pvName, testConfigService);
            }

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * test of creating channels for 1000 pvs in monitor mode
     */
    @Test
    public void create1000MonitorChannel() {
        try {

            for (int m = 1000; m < 2000; m++) {
                String pvName = pvPrefix + "test_" + m;
                ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.MONITOR,
                        60, writer, testConfigService,
                        ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
                Thread.sleep(10);
            }

            Thread.sleep(2000);
            int num = 0;
            for (int m = 1000; m < 2000; m++) {
                String pvName = pvPrefix + "test_" + m;
                PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
                        testConfigService);
                if (tempPVMetrics.isConnected())
                    num++;

            }
	        assertEquals("Only "
			        + num
			        + " of 1000 of channels in monitor mode  connected successfully", 1000, num);

            for (int m = 0; m < 1000; m++) {
                String pvName = pvPrefix + "test_" + m;
                ArchiveEngine.destoryPv(pvName, testConfigService);
            }
        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    /**
     * test of getting pv combined data of previous and current ArrayListEventStream
     */
    @Test
    public void getPVdata() {
        String pvName = pvPrefix + "test_2";
        try {

            ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
                    testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
            Thread.sleep(2000);
            ArrayListEventStream samples = testConfigService.getEngineContext().getChannelList().get(pvName).getPVData();

            assertTrue("there is no data in sample buffer", samples.size() > 0);
            ArchiveEngine.destoryPv(pvName, testConfigService);
        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }

    }
}
