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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.zip.ZipEntry;

import junit.framework.TestCase;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SlowTests;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
/**
 * test for consolidate all pb files from short term storage and medium term storage to long term storage
 * @author Luofeng Li
 *
 */
@Category(SlowTests.class)
public class ConsolidateETLJobsForOnePVTest extends TestCase {

        private static Logger logger = LogManager.getLogger(ConsolidateETLJobsForOnePVTest.class.getName());
        String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "ConsolidateETLJobsForOnePVTest";
        String shortTermFolderName=rootFolderName+"/shortTerm";
        String mediumTermFolderName=rootFolderName+"/mediumTerm";
        String longTermFolderName=rootFolderName+"/longTerm";
        String pvName = "ArchUnitTest" + "ConsolidateETLJobsForOnePVTest";
        PlainPBStoragePlugin storageplugin1;
        PlainPBStoragePlugin storageplugin2;
        PlainPBStoragePlugin storageplugin3;
        short currentYear = TimeUtils.getCurrentYear();
        ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
        private  ConfigServiceForTests configService;
        
        @Before
        public void setUp() throws Exception {
        	configService = new ConfigServiceForTests();
                if(new File(rootFolderName).exists()) {
                        FileUtils.deleteDirectory(new File(rootFolderName));
                }

                storageplugin1 = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_HOUR", configService);
                storageplugin2 = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_DAY&hold=5&gather=3", configService);
                storageplugin3 = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=LTS&rootFolder=" + longTermFolderName + "/&partitionGranularity=PARTITION_DAY&compress=ZIP_PER_PV", configService);
        }

        @After
        public void tearDown() throws Exception {
        	// FileUtils.deleteDirectory(new File(rootFolderName));
        	configService.shutdownNow();
        }
        @Test
        public void testAll(){
                try {
                        Consolidate();
                } catch (AlreadyRegisteredException | IOException | InterruptedException e) {
                        logger.error("Exception consolidating storage", e);
                }
        }
        
        @SuppressWarnings("deprecation")
        private void Consolidate() throws AlreadyRegisteredException, IOException, InterruptedException{
                 PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
                String[] dataStores = new String[] { storageplugin1.getURLRepresentation(), storageplugin2.getURLRepresentation(),storageplugin3.getURLRepresentation() }; 
                typeInfo.setDataStores(dataStores);
                configService.updateTypeInfoForPV(pvName, typeInfo);
                configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
                configService.getETLLookup().manualControlForUnitTests();
                //generate datas of 10 days PB file 2012_01_01.pb  to 2012_01_10.pb
                int dayCount=10;
                for(int day = 0; day < dayCount; day++) {
                                logger.debug("Generating data for day " + 1);
                                int startofdayinseconds = day*24*60*60;
                                int runsperday = 12;
                                int eventsperrun = 24*60*60/runsperday;
                                for(int currentrun = 0; currentrun < runsperday; currentrun++) {
                                        try(BasicContext context = new BasicContext()) {
                                                logger.debug("Generating data for run " + currentrun);
                                                
                                                YearSecondTimestamp yts = new YearSecondTimestamp(currentYear, (day+1)*24*60*60, 0);
                                                Timestamp etlTime = TimeUtils.convertFromYearSecondTimestamp(yts);
                                                logger.debug("Running ETL as if it were " + TimeUtils.convertToHumanReadableString(etlTime));
                                                ETLExecutor.runETLs(configService, etlTime);
                                                ArrayListEventStream testData = new ArrayListEventStream(eventsperrun, new RemotableEventStreamDesc(type, pvName, currentYear));
                                                for(int secondsinrun = 0; secondsinrun < eventsperrun; secondsinrun++) {
                                                        testData.add(new SimulationEvent(startofdayinseconds + currentrun*eventsperrun + secondsinrun, currentYear, type, new ScalarValue<Double>((double) secondsinrun)));
                                                }
                                                storageplugin1.appendData(context, pvName, testData);
                                        }
                                        // Sleep for a couple of seconds so that the modification times are different.
                                        Thread.sleep(2*1000);
                                }
                }// end for 
                
                
                File shortTermFIle=new File(shortTermFolderName);
                File mediumTermFIle=new File(mediumTermFolderName);
                //File longTermFIle=new File(longTermFolderName);
                
                String[]filesShortTerm= shortTermFIle.list();
                String[]filesMediumTerm= mediumTermFIle.list();
                assertTrue("there should be PB files int short term storage but there is no ",filesShortTerm.length!=0);
                assertTrue("there should be PB files int medium term storage but there is no ",filesMediumTerm.length!=0);
               //ArchUnitTestConsolidateETLJobsForOnePVTest:_pb.zip
                File zipFileOflongTermFile=new File(longTermFolderName+"/"+pvName+":_pb.zip");
                assertTrue(longTermFolderName+"/"+pvName+":_pb.zip shoule exist but it doesn't",zipFileOflongTermFile.exists());
                ZipFile lastZipFile1=new ZipFile(zipFileOflongTermFile);
                Enumeration<ZipArchiveEntry> enumeration1=lastZipFile1.getEntries();
                int ss=0;
                while(enumeration1.hasMoreElements()){
                  enumeration1.nextElement();
                  ss++;
                }
                assertTrue("the zip file of "+longTermFolderName+"/"+pvName+":_pb.zip should contain pb files less than "+dayCount+",but the number is "+ss,ss<dayCount);
                // consolidate
                String storageName="LTS";
                Timestamp oneYearLaterTimeStamp=TimeUtils.convertFromEpochSeconds(TimeUtils.getCurrentEpochSeconds()+365*24*60*60, 0);
                // The ConfigServiceForTests automatically adds a ETL Job for each PV. For consolidate, we need to have "paused" the PV; we fake this by deleting the jobs.
                configService.getETLLookup().deleteETLJobs(pvName);
                ETLExecutor.runPvETLsBeforeOneStorage(configService, oneYearLaterTimeStamp, pvName, storageName);
                // make sure there are no pb files in short term storage , medium term storage and all files in long term storage
                Thread.sleep(4000);
               String[]filesShortTerm2= shortTermFIle.list();
                String[]filesMediumTerm2= mediumTermFIle.list();
                assertTrue("there should be no files int short term storage but there are still "+filesShortTerm2.length+"PB files",filesShortTerm2.length==0);
                assertTrue("there should be no files int medium term storage but there are still "+filesMediumTerm2.length+"PB files",filesMediumTerm2.length==0);
               //ArchUnitTestConsolidateETLJobsForOnePVTest:_pb.zip
                File zipFileOflongTermFile2=new File(longTermFolderName+"/"+pvName+":_pb.zip");
                assertTrue(longTermFolderName+"/"+pvName+":_pb.zip shoule exist but it doesn't",zipFileOflongTermFile2.exists());
              
                ZipFile lastZipFile=new ZipFile(zipFileOflongTermFile2);
                Enumeration<ZipArchiveEntry> enumeration=lastZipFile.getEntries();
                ZipEntry zipEntry=null;
                HashMap<String,String> fileNameMap=new HashMap<String,String>();
                while(enumeration.hasMoreElements()){
                  zipEntry=(ZipEntry)enumeration.nextElement();
                  
                  String fileNameTemp=zipEntry.getName();
                  logger.info("fileName1="+fileNameTemp);
                  
                  int indexPB=fileNameTemp.indexOf(".pb");
                  int indexDate=indexPB-5;
                  String dateFileName=fileNameTemp.substring(indexDate, indexPB);
                  fileNameMap.put(dateFileName, dateFileName);
                  //System.out.println("fileName="+dateFileName);
                 logger.info("fileName="+dateFileName);
                }
                
                assertTrue("The number of files should be "+dayCount+", acutuallly, it is "+fileNameMap.size(),fileNameMap.size()==dayCount);
                Date beinningDate=new Date();
                beinningDate.setYear(currentYear-1);
                beinningDate.setMonth(11);
                beinningDate.setDate(31);
                logger.info("currentYear="+currentYear);
                logger.info("beinningDate="+beinningDate);
                Calendar calendarBeingining=Calendar.getInstance();
                calendarBeingining.setTime(beinningDate);
                SimpleDateFormat df=new SimpleDateFormat("MM_dd");
                for(int m=0;m<dayCount;m++){
                  calendarBeingining.add(Calendar.DAY_OF_MONTH, 1);
                  String fileNameTemp1=df.format(calendarBeingining.getTime());
                  logger.info("fileNameTemp1="+fileNameTemp1);
                  assertTrue("the file  whose name is like "+pvName+":"+currentYear+"_"+fileNameTemp1+".pb should exist,but it doesn't",fileNameMap.get(fileNameTemp1)!=null);
                }
               
         

        }
        
}