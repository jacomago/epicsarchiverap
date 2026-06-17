/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *****
 */
package org.epics.archiverappliance.mgmt.archivepv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.CPStaticData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Entry point for the default capacity planning: it decides which appliance a new PV is assigned to.
 *
 * <p>This class is the thin orchestrator. It gathers the data the decision needs (which involves
 * HTTP calls to the appliances in the cluster) and then delegates the actual decision to the pure,
 * unit-testable {@link CapacityPlanningAlgorithm}. When the algorithm cannot decide, the fallback
 * lives here (see {@link #pickApplianceForPV}).
 *
 * @author luofeng
 */
public class CapacityPlanningBPL {
    private static final Logger logger = LogManager.getLogger(CapacityPlanningBPL.class.getName());

    /**
     * The maximum percentage of capacity (storage, writer time and ETL time) that an appliance may
     * use before it is considered unavailable for new PVs.
     */
    private static final float percentageLimitation = 80;

    /**
     * Pick the appliance that the given PV should be assigned to.
     *
     * @param pvName the name of the PV.
     * @param configService the local configService.
     * @param pvTypeInfo the pvTypeInfo of this PV.
     * @return the ApplianceInfo of the appliance that this PV will be added to. On error, returns the
     *     local appliance.
     * @throws IOException if an error occurs gathering capacity planning data.
     */
    public static ApplianceInfo pickApplianceForPV(String pvName, ConfigService configService, PVTypeInfo pvTypeInfo)
            throws IOException {
        try {
            Map<String, Integer> destinationPartitionSeconds =
                    computeDestinationPartitionSeconds(pvTypeInfo, configService);

            CPStaticData cpStaticData = CapacityPlanningData.getMetricsForAppliances(configService);
            Map<ApplianceInfo, CapacityPlanningData> appliances = cpStaticData.cpApplianceMetrics;

            // Fetch the aggregate difference once per appliance so the decision itself does no I/O.
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences = new HashMap<>();
            for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
                aggregateDifferences.put(
                        entry.getKey(), entry.getValue().getApplianceAggregateDifferenceFromLastFetch(configService));
            }

            float secondsToBuffer = PVTypeInfo.getSecondsToBuffer(configService);

            Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                    pvName,
                    appliances,
                    aggregateDifferences,
                    destinationPartitionSeconds,
                    secondsToBuffer,
                    percentageLimitation);

            return chosen.orElseGet(configService::getMyApplianceInfo);
        } catch (Exception e) {
            logger.error("Exception during capacity planning, returning this appliance", e);
            return configService.getMyApplianceInfo();
        }
    }

    /**
     * Map each destination the PV writes to (that supports the storage API) to the approximate number
     * of seconds of data per chunk for that destination.
     */
    private static Map<String, Integer> computeDestinationPartitionSeconds(
            PVTypeInfo pvTypeInfo, ConfigService configService) throws IOException {
        Map<String, Integer> destinationPartitionSeconds = new HashMap<>();
        for (String dataStore : pvTypeInfo.getDataStores()) {
            ETLSource etlSource = StoragePluginURLParser.parseETLSource(dataStore, configService);
            if (etlSource == null) {
                logger.debug("the ETLSource of " + dataStore + " is null");
                continue;
            }

            ETLDest etlDest = StoragePluginURLParser.parseETLDest(dataStore, configService);
            if (etlDest instanceof StorageMetrics) {
                int partitionSecond = etlSource.getPartitionGranularity().getApproxSecondsPerChunk();
                String destinationName = ((StorageMetrics) etlDest).getName();
                destinationPartitionSeconds.put(destinationName, partitionSecond);
            }
        }
        return destinationPartitionSeconds;
    }
}
