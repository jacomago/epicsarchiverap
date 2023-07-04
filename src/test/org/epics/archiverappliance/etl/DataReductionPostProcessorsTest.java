/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.SingleForkTests;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.*;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Variation of DataReductionDailyETLTest; except we test multiple post processors
 * @author mshankar
 *
 */
@RunWith(Parameterized.class)
@Category(SingleForkTests.class)
public class DataReductionPostProcessorsTest {
	private static final Logger logger = LogManager.getLogger(DataReductionPostProcessorsTest.class);
	String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
	String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";
	String longTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/longTerm";
	private String rawPVName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + DataReductionPostProcessorsTest.class.getSimpleName();
	private String reducedPVName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + DataReductionPostProcessorsTest.class.getSimpleName() + "reduced";

	@Before
	public void setUp() throws Exception {
		cleanDataFolders();
	}

	private void cleanDataFolders() throws IOException {
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
		if(new File(mediumTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(mediumTermFolderName));
		}
		if(new File(longTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(longTermFolderName));
		}
	}

	@After
	public void tearDown() throws Exception {
		cleanDataFolders();
	}

	private String reduceDataUsing;
    @Parameterized.Parameters
	public static Collection<Object[]> postProcessors() {
		return Arrays.asList( new Object[][] {
				// No fill versions
				{"lastSample_3600"},
				{"firstSample_3600"},
				{"firstSample_600"},
				{"lastSample_600"},
				{"meanSample_3600"},
				{"meanSample_600"},
				{"meanSample_1800"},
				{"minSample_3600"},
				{"maxSample_3600"},
				{"medianSample_3600"},
				// Fill versions
				{"mean_3600"},
				{"mean_600"},
				{"mean_1800"},
				{"min_3600"},
				{"max_3600"},
				{"median_3600"},
				{"firstFill_3600"},
				{"lastFill_3600"}
				});
	}
	
	public DataReductionPostProcessorsTest(String reduceDataUsing) {
		this.reduceDataUsing = reduceDataUsing;
	}

	/**
	 * 1) Set up the raw and reduced PV's
	 * 2) Generate data in STS
	 * 3) Run ETL
	 * 4) Compare
	 */
	@Test
	public void testPostProcessor() throws Exception {
		String reduceDataUsing = this.reduceDataUsing;
		logger.info("Testing for " + this.reduceDataUsing);
		cleanDataFolders();

		ConfigServiceForTests configService = new ConfigServiceForTests( 1);
		// Set up the raw and reduced PV's
		PlainPBStoragePlugin etlSTS = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_HOUR", configService);
		PlainPBStoragePlugin etlMTS = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_DAY", configService);
		PlainPBStoragePlugin etlLTSRaw = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + longTermFolderName + "/&partitionGranularity=PARTITION_YEAR", configService);
		PlainPBStoragePlugin etlLTSReduced = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + longTermFolderName + "/&partitionGranularity=PARTITION_YEAR&reducedata=" + reduceDataUsing, configService);
		{ 
			PVTypeInfo typeInfo = new PVTypeInfo(rawPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores = new String[] { etlSTS.getURLRepresentation(), etlMTS.getURLRepresentation(), etlLTSRaw.getURLRepresentation() }; 
			typeInfo.setDataStores(dataStores);
			typeInfo.setPaused(true);
			configService.updateTypeInfoForPV(rawPVName, typeInfo);
			configService.registerPVToAppliance(rawPVName, configService.getMyApplianceInfo());
		}
		{ 
			PVTypeInfo typeInfo = new PVTypeInfo(reducedPVName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores = new String[] { etlSTS.getURLRepresentation(), etlMTS.getURLRepresentation(), etlLTSReduced.getURLRepresentation() }; 
			typeInfo.setDataStores(dataStores);
			typeInfo.setPaused(true);
			configService.updateTypeInfoForPV(reducedPVName, typeInfo);
			configService.registerPVToAppliance(reducedPVName, configService.getMyApplianceInfo());
		}
		// Control ETL manually
		configService.getETLLookup().manualControlForUnitTests();

		short currentYear = TimeUtils.getCurrentYear();

		logger.info("Testing data reduction for postprocessor " + reduceDataUsing);

		for(int day = 0; day < 40; day++) { 
			// Generate data into the STS on a daily basis
			ArrayListEventStream genDataRaw = new ArrayListEventStream(86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, rawPVName, currentYear));
			ArrayListEventStream genDataReduced = new ArrayListEventStream(86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, reducedPVName, currentYear));
			for(int second = 0; second < 86400; second++) { 
				YearSecondTimestamp ysts = new YearSecondTimestamp(currentYear, day*86400 + second, 0);
				Timestamp ts = TimeUtils.convertFromYearSecondTimestamp(ysts);
				genDataRaw.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>(second*1.0),0, 0));
				genDataReduced.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>(second*1.0),0, 0));
			}

			try(BasicContext context = new BasicContext()) {
				etlSTS.appendData(context, rawPVName, genDataRaw);
				etlSTS.appendData(context, reducedPVName, genDataReduced);
			}        	
			logger.debug("For postprocessor " + reduceDataUsing +  " done generating data into the STS for day " + day);

			// Run ETL at the end of the day
			Timestamp timeETLruns = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(currentYear, day*86400 + 86399, 0));
			ETLExecutor.runETLs(configService, timeETLruns);
			logger.debug("For postprocessor " + reduceDataUsing +  " done performing ETL as though today is " + TimeUtils.convertToHumanReadableString(timeETLruns));

			// Compare data for raw+postprocessor and reduced PV's.
			PostProcessor postProcessor = PostProcessors.findPostProcessor(reduceDataUsing);
			postProcessor.initialize(reduceDataUsing, rawPVName);

			int rawWithPPCount = 0;
			int reducedCount = 0;

			try (BasicContext context = new BasicContext()) { 
				Timestamp startTime = TimeUtils.minusDays(TimeUtils.now(), 10*366);
				Timestamp endTime = TimeUtils.plusDays(TimeUtils.now(), 10*366);
				LinkedList<Timestamp> rawTimestamps = new LinkedList<Timestamp>();
				LinkedList<Timestamp> reducedTimestamps = new LinkedList<Timestamp>();
				if(postProcessor instanceof PostProcessorWithConsolidatedEventStream) {
					List<Callable<EventStream>> callables = etlLTSRaw.getDataForPV(context, rawPVName, startTime, endTime, postProcessor);
					for(Callable<EventStream> callable : callables) { 
						callable.call();
					}
					for(Event e : ((PostProcessorWithConsolidatedEventStream) postProcessor).getConsolidatedEventStream()) { 
						rawTimestamps.add(e.getEventTimeStamp());
						rawWithPPCount++; 
					}
				} else { 
					try(EventStream rawWithPP = new CurrentThreadWorkerEventStream(rawPVName, etlLTSRaw.getDataForPV(context, rawPVName, startTime, endTime, postProcessor))) {
						for(Event e : rawWithPP) {
							rawTimestamps.add(e.getEventTimeStamp());
							rawWithPPCount++; 
						}
					}
				}
				try(EventStream reduced = new CurrentThreadWorkerEventStream(reducedPVName, etlLTSReduced.getDataForPV(context, reducedPVName, startTime, endTime))) {
					for(Event e : reduced) {
						reducedTimestamps.add(e.getEventTimeStamp());
						reducedCount++; 
					} 
				}
				
				logger.debug("For postprocessor " + reduceDataUsing +  " for day " + day + " we have " + rawWithPPCount + " raw with postprocessor events and " + reducedCount + " reduced events");
				if(rawTimestamps.size() != reducedTimestamps.size()) { 
					while(!rawTimestamps.isEmpty() || !reducedTimestamps.isEmpty()) { 
						if(!rawTimestamps.isEmpty()) logger.info("Raw/PP " + TimeUtils.convertToHumanReadableString(rawTimestamps.pop()));
						if(!reducedTimestamps.isEmpty()) logger.info("Reduced" + TimeUtils.convertToHumanReadableString(reducedTimestamps.pop()));
					}
				}
				assertTrue("For postprocessor " + reduceDataUsing +  " for day " + day + " we have " + rawWithPPCount + " rawWithPP events and " + reducedCount + " reduced events", rawWithPPCount == reducedCount);
			}
			if(day > 2) { 
				assertTrue("For postprocessor " + reduceDataUsing +  " for day " + day + ", seems like no events were moved by ETL into LTS for " + rawPVName + " Count = " + rawWithPPCount, (rawWithPPCount != 0));
				assertTrue("For postprocessor " + reduceDataUsing +  " for day " + day + ", seems like no events were moved by ETL into LTS for " + reducedPVName + " Count = " + reducedCount, (reducedCount != 0));
			}
		}        	

		configService.shutdownNow();
	}
}
