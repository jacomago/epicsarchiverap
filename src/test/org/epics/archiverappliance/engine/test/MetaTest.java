/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.ParallelEpicsIntegrationTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * test of getting meta data
 *
 * @author Luofeng Li
 */
@Category(ParallelEpicsIntegrationTests.class)
public class MetaTest {
    private static final Logger logger = LogManager.getLogger(MetaTest.class.getName());
    private static final String pvPrefix = MetaTest.class.getSimpleName();
    private static SIOCSetup ioc = null;
    private static ConfigServiceForTests testConfigService;

    @BeforeClass
    public static void setUp() throws Exception {
        ioc = new SIOCSetup(pvPrefix);
        ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests();
        Thread.sleep(3000);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testConfigService.shutdownNow();
        ioc.stopSIOC();
    }

    /**
     * test of getting meta data for one pv.
     */
    @Test
    public void singlePVMeta() {
        CountDownLatch latch = new CountDownLatch(1);

        try {

            String[] metaFied = {"MDEL", "ADEL", "RTYP"};
            ArchiveEngine.getArchiveInfo(pvPrefix + "test_0", testConfigService, metaFied, false,
		            metaInfo -> {
		                System.out.println(metaInfo.toString());
		                String MDELStr = metaInfo.getOtherMetaInfo().get(
		                        "MDEL");
		                String ADELStr = metaInfo.getOtherMetaInfo().get(
		                        "ADEL");
		                String RTYPStr = metaInfo.getOtherMetaInfo().get(
		                        "RTYP");
		                Assert.assertNotNull("MDEL of meta data should not be null", MDELStr);
		                Assert.assertNotNull("ADEL of meta data should not be null", ADELStr);
		                Assert.assertNotNull("RTYP of meta data should not be null", RTYPStr);
		                latch.countDown();
		            });

            Assert.assertTrue(latch.await(70, TimeUnit.SECONDS));

        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }

    @Test
    public void testAliasNames() {
        HashMap<String, AliasNames> aliasNames = new HashMap<String, AliasNames>();
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sine", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sine.DESC", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.DESC"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sine.HIHI", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.HIHI"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sinealias", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sinealias.DESC", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.DESC"));
        aliasNames.put(pvPrefix + "UnitTestNoNamingConvention:sinealias.HIHI", new AliasNames(pvPrefix + "UnitTestNoNamingConvention:sine.HIHI"));

        CountDownLatch latch = new CountDownLatch(6);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sine", aliasNames);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sine.DESC", aliasNames);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sine.HIHI", aliasNames);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sinealias", aliasNames);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sinealias.DESC", aliasNames);
        testAliasNamesForPV(latch, pvPrefix + "UnitTestNoNamingConvention:sinealias.HIHI", aliasNames);

        try {
            Assert.assertTrue("MetaGet did not complete for all PV's " + latch.getCount(), latch.await(90, TimeUnit.SECONDS));
        } catch (InterruptedException ex) {
            logger.error(ex);
        }

        for (String pvName : aliasNames.keySet()) {
            AliasNames aliasName = aliasNames.get(pvName);
	        Assert.assertEquals("AliasName for " + pvName + " is not " + aliasName.expectedName + ". Instead it is " + aliasName.metaGetAliasName, aliasName.expectedName, aliasName.metaGetAliasName);
	        Assert.assertEquals("NAME info hashmap for " + pvName + " is not " + aliasName.expectedName + ". Instead it is " + aliasName.metaGetOtherInfoName, aliasName.expectedName, aliasName.metaGetOtherInfoName);
        }
    }

    /**
     * Test the NAME and NAME$ for various PV's and fields of PV's
     */
    private void testAliasNamesForPV(final CountDownLatch latch, final String pvName, HashMap<String, AliasNames> aliasNames) {
        String[] metaFied = {"MDEL", "ADEL", "RTYP"};
        try {
            ArchiveEngine.getArchiveInfo(pvName, testConfigService, metaFied, false, metaInfo -> {
                latch.countDown();
                logger.info("Metadata completed for " + pvName + "aliasName " + metaInfo.getAliasName() + "Name: " + metaInfo.getOtherMetaInfo().get("NAME"));
                aliasNames.get(pvName).metaGetAliasName = metaInfo.getAliasName();
                aliasNames.get(pvName).metaGetOtherInfoName = metaInfo.getOtherMetaInfo().get("NAME");
            });
        } catch (Exception ex) {
            logger.error(ex);
	        Assert.fail("Exception thrown " + ex.getMessage());
        }
    }

    class AliasNames {
        String expectedName;
        String metaGetAliasName;
        String metaGetOtherInfoName;

        AliasNames(String expectedName) {
            this.expectedName = expectedName;
        }
    }


}
