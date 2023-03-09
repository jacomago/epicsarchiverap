/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.ParallelEpicsIntegrationTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

/**
 * test of year changing. When year changes, we should create a new ArrayListEventStream
 *
 * @author Luofeng Li
 */
@Category(ParallelEpicsIntegrationTests.class)
public class YearListenerTest {
    private static final Logger logger = LogManager.getLogger(YearListenerTest.class.getName());
    private final String pvPrefix = YearListenerTest.class.getSimpleName();
    private SIOCSetup ioc = null;
    private DefaultConfigService testConfigService;
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

        testConfigService.shutdownNow();
        ioc.stopSIOC();

    }

    @Test
    public void singlePvYearChangeListener() {
        //change your time of your computer to 2011-12-31 23:58:00
        try {

            ArchiveEngine.archivePV(pvPrefix + "test_0", 2,
                    SamplingMethod.SCAN,
                    10, writer,
                    testConfigService,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    null, false, false);
        } catch (Exception e) {
            //
            logger.error("Exception", e);
        }
    }


}
