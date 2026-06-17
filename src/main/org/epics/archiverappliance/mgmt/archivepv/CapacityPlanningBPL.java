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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
    private static final Logger configlogger = LogManager.getLogger("config." + CapacityPlanningBPL.class.getName());

    /**
     * Installation property overriding the maximum percentage of capacity (storage, writer time and
     * ETL time) that an appliance may use before it is considered unavailable for new PVs.
     */
    static final String PERCENTAGE_LIMITATION_PROPERTY =
            "org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningBPL.percentageLimitation";

    private static final float DEFAULT_PERCENTAGE_LIMITATION = 80;

    private static final Random RANDOM = new Random();

    /**
     * PVs assigned by this node since the measured metrics were last (re)fetched, per appliance
     * identity. Reconciled against the freshly-fetched aggregate PV count so that, during a burst,
     * assignments not yet visible in the aggregate still steer the planner away from piling onto one
     * appliance. Reset whenever the measured metrics are refetched.
     */
    private static final ConcurrentHashMap<String, Integer> localAssignmentsSinceFetch = new ConcurrentHashMap<>();

    private static volatile Instant localAssignmentsBaseline;

    /**
     * Pick the appliance that the given PV should be assigned to.
     *
     * @param pvName the name of the PV.
     * @param configService the local configService.
     * @param pvTypeInfo the pvTypeInfo of this PV.
     * @return the ApplianceInfo of the appliance that this PV will be added to. When no appliance can
     *     be decided (e.g. all appliances are over capacity), a random appliance from the cluster is
     *     returned. On error gathering data, the local appliance is returned.
     * @throws IOException if an error occurs gathering capacity planning data.
     */
    public static ApplianceInfo pickApplianceForPV(String pvName, ConfigService configService, PVTypeInfo pvTypeInfo)
            throws IOException {
        try {
            Map<String, Integer> destinationPartitionSeconds =
                    computeDestinationPartitionSeconds(pvTypeInfo, configService);
            float pvStorageRate = pvTypeInfo.getComputedStorageRate();

            CapacityPlanningMetricsProvider.Snapshot snapshot = CapacityPlanningMetricsProvider.gather(configService);
            Map<ApplianceInfo, CapacityPlanningData> appliances = snapshot.appliances();
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences = snapshot.aggregateDifferences();

            float secondsToBuffer = PVTypeInfo.getSecondsToBuffer(configService);
            Map<String, Integer> inFlight =
                    inFlightByApplianceIdentity(snapshot.metricsTimestamp(), appliances, aggregateDifferences);

            Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                    pvName,
                    appliances,
                    aggregateDifferences,
                    destinationPartitionSeconds,
                    pvStorageRate,
                    secondsToBuffer,
                    getPercentageLimitation(configService),
                    inFlight,
                    RANDOM);

            if (chosen.isPresent()) {
                return recordAssignment(chosen.get());
            }

            // The planner could not decide (e.g. every appliance is over capacity). Spread the load
            // by picking a random appliance from the cluster rather than always defaulting to self.
            List<ApplianceInfo> clusterAppliances = new ArrayList<>();
            configService.getAppliancesInCluster().forEach(clusterAppliances::add);
            ApplianceInfo randomAppliance = CapacityPlanningAlgorithm.randomAppliance(clusterAppliances, RANDOM);
            if (randomAppliance != null) {
                configlogger.error("Capacity planning could not decide an appliance for " + pvName
                        + "; picking the random appliance " + randomAppliance.getIdentity());
                return recordAssignment(randomAppliance);
            }
            return configService.getMyApplianceInfo();
        } catch (Exception e) {
            logger.error("Exception during capacity planning, returning this appliance", e);
            return configService.getMyApplianceInfo();
        }
    }

    /**
     * The maximum percentage of capacity an appliance may use before it is considered unavailable for
     * new PVs, from {@value #PERCENTAGE_LIMITATION_PROPERTY} (default 80).
     */
    static float getPercentageLimitation(ConfigService configService) {
        return Float.parseFloat(configService
                .getInstallationProperties()
                .getProperty(PERCENTAGE_LIMITATION_PROPERTY, Float.toString(DEFAULT_PERCENTAGE_LIMITATION)));
    }

    /**
     * Per-appliance count of PVs this node assigned but the fetched aggregate has not caught up to.
     * The local counters reset whenever the measured metrics are refetched; the aggregate's own
     * PV-count difference is then subtracted, so steady-state (already-propagated) assignments
     * contribute zero and only a genuine in-flight backlog steers the decision.
     */
    private static Map<String, Integer> inFlightByApplianceIdentity(
            Instant metricsTimestamp,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences) {
        if (!metricsTimestamp.equals(localAssignmentsBaseline)) {
            localAssignmentsSinceFetch.clear();
            localAssignmentsBaseline = metricsTimestamp;
        }
        Map<String, Integer> inFlight = new HashMap<>();
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            String identity = entry.getKey().getIdentity();
            int assignedLocally = localAssignmentsSinceFetch.getOrDefault(identity, 0);
            int reflectedInAggregate =
                    (int) aggregateDifferences.get(entry.getKey()).getTotalPVCount();
            inFlight.put(identity, Math.max(0, assignedLocally - reflectedInAggregate));
        }
        return inFlight;
    }

    private static ApplianceInfo recordAssignment(ApplianceInfo appliance) {
        localAssignmentsSinceFetch.merge(appliance.getIdentity(), 1, Integer::sum);
        return appliance;
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
