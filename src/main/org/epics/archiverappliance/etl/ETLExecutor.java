package org.epics.archiverappliance.etl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.AppliancePVsView;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginConfigView;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.common.ETLMetricsIntoStore;
import org.epics.archiverappliance.etl.common.ETLStage;
import org.epics.archiverappliance.etl.common.ETLStages;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Run ETLs for one PV; mostly for unit tests..
 * @author luofeng
 *
 */
public class ETLExecutor {
    private static Logger logger = LogManager.getLogger(ETLExecutor.class.getName());

    /**
     * This should only be called from within unit tests...
     * @param appliancePVs AppliancePVsView
     * @param timeETLruns Instant
     * @throws IOException  &emsp;
     */
    public static void runETLs(AppliancePVsView appliancePVs, Instant timeETLruns) throws IOException {
        for (String pvName : appliancePVs.getPVsForThisAppliance()) {
            ETLStages etlStages = PBThreeTierETLPVLookup.of(appliancePVs).getETLStages(pvName);
            if (etlStages == null) {
                logger.debug("Skipping ETL for {} as it has no ETL stages", pvName);
                continue;
            }
            logger.debug(
                    "Running ETL for {} with stages {}",
                    pvName,
                    etlStages.getStages().size());
            try (ScheduledThreadPoolExecutor scheduleWorker = new ScheduledThreadPoolExecutor(1)) {
                try (ExecutorService theWorker = Executors.newVirtualThreadPerTaskExecutor()) {
                    Future<?> f = scheduleWorker.submit(new Runnable() {
                        @Override
                        public void run() {
                            etlStages.runAsIfAtTime(timeETLruns);
                        }
                    });
                    f.get();
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
            if (etlStages.getAnyExceptionFromLastRun() != null)
                throw new IOException(etlStages.getAnyExceptionFromLastRun());
        }
    }

    /**
     * Run ETL for one PV until one storage; used in consolidate...
     * Make sure that the regular ETL has been paused..
     * @param storageConfig StoragePluginConfigView
     * @param applianceLifecycle ApplianceLifecycle
     * @param timeETLRuns Instant
     * @param pvName The name of PV.
     * @param storageName   &emsp;
     * @throws IOException  &emsp;
     */
    public static void runPvETLsBeforeOneStorage(
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final Instant timeETLRuns,
            final String pvName,
            final String storageName)
            throws IOException {
        PBThreeTierETLPVLookup etlLookup = PBThreeTierETLPVLookup.of(applianceLifecycle);
        if (etlLookup.getETLStages(pvName) != null) {
            throw new IOException(
                    "The pv " + pvName + " has entries in PBThreeTierETLPVLookup. Please remove these first");
        }
        PVTypeInfo pvTypeInfo = storageConfig.getTypeInfoForPV(pvName);
        String[] dataStores = pvTypeInfo.getDataStores();
        if (dataStores == null || dataStores.length < 2) {
            throw new IOException("The pv " + pvName + " has not enough stores.");
        }

        try (ScheduledThreadPoolExecutor scheduleWorker = new ScheduledThreadPoolExecutor(1)) {
            try (ExecutorService theWorker = Executors.newVirtualThreadPerTaskExecutor()) {
                final ETLStages etlStages = new ETLStages(pvName, theWorker, storageConfig, applianceLifecycle);
                for (int i = 1; i < dataStores.length; i++) {
                    String destStr = dataStores[i];
                    ETLDest etlDest = StoragePluginURLParser.parseETLDest(destStr, storageConfig);
                    StorageMetrics storageMetricsAPIDest = (StorageMetrics) etlDest;
                    String identifyDest = storageMetricsAPIDest.getName();
                    logger.info("storage name:" + identifyDest);
                    String sourceStr = dataStores[i - 1];
                    ETLSource etlSource = StoragePluginURLParser.parseETLSource(sourceStr, storageConfig);
                    etlStages.addStage(new ETLStage(
                            pvName,
                            pvTypeInfo.getDBRType(),
                            etlSource,
                            etlDest,
                            i - 1,
                            new ETLMetricsIntoStore(etlDest.getName()),
                            PBThreeTierETLPVLookup.determineOutOfSpaceHandling(storageConfig)));
                    if (storageName.equals(identifyDest)) {
                        break;
                    }
                }
                Future<?> f = scheduleWorker.submit(new Runnable() {
                    @Override
                    public void run() {
                        etlStages.runAsIfAtTime(timeETLRuns);
                    }
                });
                f.get();
            }
        } catch (Exception ex) {
            logger.error("Exception consolidating data for PV " + pvName, ex);
        }
    }
    /**
     * Run ETL for one PV until one storage; used in consolidate...
     * Make sure that the regular ETL has been paused..
     * @param storageConfig StoragePluginConfigView
     * @param applianceLifecycle ApplianceLifecycle
     * @param pvName The name of PV.
     * @param storageName   &emsp;
     * @throws IOException  &emsp;
     */
    public static void moveDataFromOneStorageToAnother(
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final String pvName,
            final String storageName,
            final String destStr)
            throws IOException {
        PVTypeInfo pvTypeInfo = storageConfig.getTypeInfoForPV(pvName);
        String[] dataStores = pvTypeInfo.getDataStores();
        if (dataStores == null) {
            throw new IOException("The pv " + pvName + " has not enough stores.");
        }

        ETLSource oldDataSource = null;
        for (String dataStore : dataStores) {
            ETLSource etlSource = StoragePluginURLParser.parseETLSource(dataStore, storageConfig);

            if (storageName.equals(etlSource.getName())) {
                oldDataSource = etlSource;
            }
        }
        try (ScheduledThreadPoolExecutor scheduleWorker = new ScheduledThreadPoolExecutor(1)) {
            try (ExecutorService theWorker = Executors.newVirtualThreadPerTaskExecutor()) {
                final ETLStages etlStages = new ETLStages(pvName, theWorker, storageConfig, applianceLifecycle);
                ETLDest etlDest = StoragePluginURLParser.parseETLDest(destStr, storageConfig);
                StorageMetrics storageMetricsAPIDest = (StorageMetrics) etlDest;
                String identifyDest = storageMetricsAPIDest.getName();
                logger.info("storage name:" + identifyDest);
                etlStages.addStage(new ETLStage(
                        pvName,
                        pvTypeInfo.getDBRType(),
                        oldDataSource,
                        etlDest,
                        -1,
                        new ETLMetricsIntoStore(etlDest.getName()),
                        PBThreeTierETLPVLookup.determineOutOfSpaceHandling(storageConfig)));

                Future<?> f = scheduleWorker.submit(etlStages::runAll);
                f.get(24, TimeUnit.HOURS);
            }
        } catch (Exception ex) {
            logger.error("Exception consolidating data for PV " + pvName, ex);
        }
    }
}
