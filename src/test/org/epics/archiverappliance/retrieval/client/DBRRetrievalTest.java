/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedList;

/**
 * Unit test to make sure the client retrieval libraries can retrieval all the DBR types.
 * @author mshankar
 *
 */
@Tag("integration")
public class DBRRetrievalTest {
    private static final Logger logger = LogManager.getLogger(DBRRetrievalTest.class.getName());
    private final LinkedList<DataDBR> dataDBRs = new LinkedList<DataDBR>();
    private final short currentYear = TimeUtils.getCurrentYear();
    TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeEach
    public void setUp() throws Exception {

        for (PlainStorageType plainStorageType : PlainStorageType.values()) {
            for (ArchDBRTypes type : ArchDBRTypes.values()) {
                dataDBRs.add(new DataDBR(
                        ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX
                                + plainStorageType
                                + (type.isWaveForm() ? "V_" : "S_")
                                + type.getPrimitiveName(),
                        type,
                        plainStorageType));
            }
        }

        for (DataDBR dataDBR : dataDBRs) {
            GenerateData.generateSineForPV(
                    dataDBR.pvName,
                    0,
                    dataDBR.type,
                    dataDBR.plainStorageType,
                    TimeUtils.convertFromISO8601String(currentYear + "-02-01T08:00:00.000Z"),
                    TimeUtils.convertFromISO8601String(currentYear + "-02-02T08:00:00.000Z"));
        }
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
    }

    @Test
    public void testGetDataForDBRs() {
        RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream(
                "http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT + "/retrieval/data/getData.raw");
        Instant start = TimeUtils.convertFromISO8601String(currentYear + "-02-01T08:00:00.000Z");
        Instant end = TimeUtils.convertFromISO8601String(currentYear + "-02-02T08:00:00.000Z");

        for (DataDBR dataDBR : dataDBRs) {
            EventStream stream = null;
            try {
                logger.info("Testing retrieval for DBR " + dataDBR.type.toString());
                stream = rawDataRetrieval.getDataForPVS(
                        new String[] {dataDBR.pvName},
                        start,
                        end,
                        desc -> logger.info("Getting data for PV " + desc.getPvName()));

                long previousEpochSeconds = 0;

                // Make sure we get the DBR type we expect
                Assertions.assertEquals(stream.getDescription().getArchDBRType(), dataDBR.type);

                // We are making sure that the stream we get back has times in sequential order...
                for (Event e : stream) {
                    long actualSeconds = e.getEpochSeconds();
                    Assertions.assertTrue(actualSeconds >= previousEpochSeconds);
                    previousEpochSeconds = actualSeconds;
                }
            } finally {
                if (stream != null)
                    try {
                        stream.close();
                        stream = null;
                    } catch (Throwable t) {
                    }
            }
        }
    }

    private record DataDBR(String pvName, ArchDBRTypes type, PlainStorageType plainStorageType) {}
}
