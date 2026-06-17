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
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.ETLMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * The pure decision logic for capacity planning, separated from data acquisition so it can be unit
 * tested. Given the measured per-appliance metrics (already fetched) plus the storage requirements
 * of the PV being added, it decides which appliance the PV should go to.
 *
 * <p>The methods deliberately take only plain data (no {@code ConfigService}, no I/O) so the
 * decision can be exercised with in-memory {@link CapacityPlanningData} and {@link ETLMetrics}.
 *
 * <p>An empty {@link Optional} means "could not decide" — the caller decides what to do in that
 * case (see {@link CapacityPlanningBPL}).
 */
class CapacityPlanningAlgorithm {
    private static final Logger logger = LogManager.getLogger(CapacityPlanningAlgorithm.class.getName());
    private static final Logger configlogger =
            LogManager.getLogger("config." + CapacityPlanningAlgorithm.class.getName());

    private CapacityPlanningAlgorithm() {}

    /** Decide with no in-flight assignment information. */
    static Optional<ApplianceInfo> decide(
            String pvName,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences,
            Map<String, Integer> destinationPartitionSeconds,
            float pvStorageRate,
            float secondsToBuffer,
            float percentageLimitation,
            Random random) {
        return decide(
                pvName,
                appliances,
                aggregateDifferences,
                destinationPartitionSeconds,
                pvStorageRate,
                secondsToBuffer,
                percentageLimitation,
                Map.of(),
                random);
    }

    /**
     * Decide which appliance a PV should be assigned to.
     *
     * @param pvName the PV being added (used for log messages only).
     * @param appliances measured capacity planning data per appliance.
     * @param aggregateDifferences the appliance aggregate info difference since the metrics were
     *     last fetched, per appliance (pre-fetched by the caller).
     * @param destinationPartitionSeconds approximate seconds per chunk for each destination the PV
     *     writes to.
     * @param secondsToBuffer the engine write period used to compute writer thread usage.
     * @param percentageLimitation the maximum percentage of any resource an appliance may use.
     * @param inFlightByIdentity count, per appliance identity, of PVs assigned locally but not yet
     *     reflected in the fetched aggregate — used to keep a burst from piling onto one appliance.
     * @param random source of randomness for picking among the appliances that fit.
     * @return the chosen appliance, or empty if no appliance could be decided.
     */
    static Optional<ApplianceInfo> decide(
            String pvName,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences,
            Map<String, Integer> destinationPartitionSeconds,
            float pvStorageRate,
            float secondsToBuffer,
            float percentageLimitation,
            Map<String, Integer> inFlightByIdentity,
            Random random) {

        // Cold start: if NO appliance has measured ETL metrics yet, the capacity math cannot run,
        // so fall back to balancing by the lowest current total data rate.
        if (allAppliancesLackEtlMetrics(appliances)) {
            logger.debug("No appliance has ETL metrics yet; balancing by data rate instead.");
            return pickByLowestDataRate(appliances, aggregateDifferences);
        }

        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            CapacityPlanningData cpMetrics = entry.getValue();
            // An appliance with no measured ETL metrics is not a valid target for the PV (it is not
            // ready / reachable), so exclude it rather than letting it dilute the decision.
            if (cpMetrics.getEtlMetrics().isEmpty()) {
                cpMetrics.setAvailable(false);
                logger.debug(entry.getKey().getIdentity() + " has no ETL metrics; excluding it as a candidate.");
                continue;
            }
            markAvailability(
                    pvName,
                    cpMetrics,
                    aggregateDifferences.get(entry.getKey()),
                    destinationPartitionSeconds,
                    pvStorageRate,
                    secondsToBuffer,
                    percentageLimitation);
        }

        List<ApplianceInfo> fitAppliances = new ArrayList<>();
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            if (entry.getValue().isAvailable()) {
                fitAppliances.add(entry.getKey());
            }
        }
        if (fitAppliances.isEmpty()) {
            configlogger.error("There is no appliance available for " + pvName);
            return Optional.empty();
        }

        // The gate above already removed anything over capacity. Among the rest, use
        // power-of-two-choices: this spreads bursts (it does not always pick the same "best"
        // appliance) yet still biases away from the busier node without trusting the hourly-cached
        // metrics to finely rank every candidate.
        return Optional.of(pickByHeadroom(fitAppliances, appliances, inFlightByIdentity, random));
    }

    /**
     * Power-of-two-choices: sample two fit appliances at random and keep the one with more headroom.
     * An appliance with fewer in-flight assignments wins outright (so a burst spreads before the
     * fetched metrics catch up); ties break on the lower projected usage.
     */
    static ApplianceInfo pickByHeadroom(
            List<ApplianceInfo> fitAppliances,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<String, Integer> inFlightByIdentity,
            Random random) {
        ApplianceInfo first = fitAppliances.get(random.nextInt(fitAppliances.size()));
        ApplianceInfo second = fitAppliances.get(random.nextInt(fitAppliances.size()));
        int firstInFlight = inFlightByIdentity.getOrDefault(first.getIdentity(), 0);
        int secondInFlight = inFlightByIdentity.getOrDefault(second.getIdentity(), 0);
        if (firstInFlight != secondInFlight) {
            return firstInFlight < secondInFlight ? first : second;
        }
        return projectedUsage(appliances.get(first)) <= projectedUsage(appliances.get(second)) ? first : second;
    }

    /**
     * The appliance's most-constrained resource after the PV is added: the larger of the projected
     * writer percentage and the projected ETL time percentage across its stores. Lower means more
     * headroom.
     */
    static double projectedUsage(CapacityPlanningData cpMetrics) {
        double usage = cpMetrics.getPercentageTimeForWriter();
        for (ETLMetrics destinationMetrics : cpMetrics.getEtlMetrics().values()) {
            usage = Math.max(usage, destinationMetrics.estimateETLtimePercentageAfterPVadded);
        }
        return usage;
    }

    static boolean allAppliancesLackEtlMetrics(Map<ApplianceInfo, CapacityPlanningData> appliances) {
        for (CapacityPlanningData cpData : appliances.values()) {
            if (!cpData.getEtlMetrics().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cold-start heuristic: choose the appliance with the lowest projected total data rate (current
     * rate plus the aggregate difference since the last fetch).
     */
    static Optional<ApplianceInfo> pickByLowestDataRate(
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences) {
        ApplianceAndTotalRate best = null;
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            ApplianceInfo applianceInfo = entry.getKey();
            float totalDataRate =
                    (float) aggregateDifferences.get(applianceInfo).getTotalStorageRate()
                            + entry.getValue().getCurrentTotalStorageRate();
            ApplianceAndTotalRate candidate = new ApplianceAndTotalRate(applianceInfo, totalDataRate);
            if (best == null || candidate.getTotalDataRate() < best.getTotalDataRate()) {
                best = candidate;
            }
        }
        return best == null ? Optional.empty() : Optional.of(best.getAppInfo());
    }

    /**
     * The storage the PV is estimated to add to a destination: the pending aggregate impact for that
     * destination (other PVs added since the metrics were fetched) plus this PV's own contribution
     * over one partition ({@code pvStorageRate * partitionSeconds}).
     */
    static long estimatedStorageForDestination(
            Map<String, Long> aggregateStorageImpact,
            String destinationName,
            float pvStorageRate,
            int partitionSeconds) {
        return aggregateStorageImpact.getOrDefault(destinationName, 0L) + (long) (pvStorageRate * partitionSeconds);
    }

    /**
     * Mark a single appliance available or not, based on whether adding the PV would exceed the
     * storage, writer time or ETL time limit. Also records the per-appliance writer percentage and
     * per-destination ETL time percentage that the normalization step reads back.
     */
    static void markAvailability(
            String pvName,
            CapacityPlanningData cpMetrics,
            ApplianceAggregateInfo aggregateInfo,
            Map<String, Integer> destinationPartitionSeconds,
            float pvStorageRate,
            float secondsToBuffer,
            float percentageLimitation) {
        cpMetrics.setAvailable(true);

        Map<String, ETLMetrics> etlMetrics = cpMetrics.getEtlMetrics();
        float totalDataRate = cpMetrics.getCurrentTotalStorageRate();
        float totalDataRateForPvAdded = (float) aggregateInfo.getTotalStorageRate();
        HashMap<String, Long> aggregateStorageImpact = aggregateInfo.getTotalStorageImpact();

        // Record the estimated storage the PV adds to each destination this appliance hosts.
        for (Map.Entry<String, Integer> destination : destinationPartitionSeconds.entrySet()) {
            ETLMetrics destinationMetrics = etlMetrics.get(destination.getKey());
            if (destinationMetrics != null) {
                destinationMetrics.estimateStoragePVadded = estimatedStorageForDestination(
                        aggregateStorageImpact, destination.getKey(), pvStorageRate, destination.getValue());
            }
        }

        // Storage: the appliance is unavailable if the estimated storage for any destination the PV
        // writes to exceeds the storage available there.
        for (ETLMetrics destinationMetrics : etlMetrics.values()) {
            String destinationName = destinationMetrics.identity;
            long availableStorage = destinationMetrics.etlStorageAvailable;
            Integer partitionSeconds = destinationPartitionSeconds.get(destinationName);
            if (partitionSeconds != null) {
                long estimateStorageSize = estimatedStorageForDestination(
                        aggregateStorageImpact, destinationName, pvStorageRate, partitionSeconds);
                if (estimateStorageSize > availableStorage) {
                    cpMetrics.setAvailable(false);
                    configlogger.error("There is not enough storage to accommodate " + pvName + " for "
                            + destinationName + ". Estimated storage for " + pvName + " is " + estimateStorageSize
                            + " while available storage is " + availableStorage);
                }
            }
            if (!cpMetrics.isAvailable()) {
                break;
            }
        }
        if (!cpMetrics.isAvailable()) {
            return;
        }

        // Writer: the engine write thread must have enough time left to flush the PV to short term
        // storage.
        float currentUsedWriterPercentage = cpMetrics.getEngineWriteThreadUsage(secondsToBuffer);
        if (currentUsedWriterPercentage > percentageLimitation) {
            cpMetrics.setAvailable(false);
            configlogger.error("There is not enough time left for writer to write " + pvName
                    + " into short term storage. Estimated writing percentage for " + pvName + " is "
                    + currentUsedWriterPercentage + " while the percentage limitation is " + percentageLimitation);
        }
        // Scale the measured writer usage by how much the data rate would grow. A fresh appliance
        // with no current data rate has nothing to scale, so use the measured usage as-is rather than
        // dividing by zero (which would yield Inf/NaN and wrongly exclude or poison the appliance).
        float percentageForWriter = totalDataRate > 0
                ? currentUsedWriterPercentage * (totalDataRateForPvAdded + totalDataRate) / totalDataRate
                : currentUsedWriterPercentage;
        cpMetrics.setPercentageTimeForWriter(percentageForWriter);
        if (percentageForWriter > percentageLimitation) {
            cpMetrics.setAvailable(false);
            configlogger.error("There is not enough time left for writer to write " + pvName
                    + " into short term storage. Estimated writing percentage for " + pvName + " is "
                    + percentageForWriter + " while the percentage limitation is " + percentageLimitation);
        }

        // ETL: normalize the ETL time taken for each store after the PV is added and check the limit.
        //   ETL time after the PV is added = ((estimateStoragePVadded + usedStorage) / usedStorage) * etlTimeTaken
        for (ETLMetrics destinationMetrics : etlMetrics.values()) {
            long storageUsed = destinationMetrics.totalSpace - destinationMetrics.etlStorageAvailable;
            double etlTimePercentage = destinationMetrics.etlTimeTaken;
            if (etlTimePercentage > percentageLimitation) {
                cpMetrics.setAvailable(false);
                configlogger.error("There is not enough time left for ETL to write " + pvName + " into "
                        + destinationMetrics.identity + ". Estimated percentage time is " + etlTimePercentage
                        + " while the percentage limitation is " + percentageLimitation);
            }

            // Scale the measured ETL time by how much the stored data would grow. An empty store has
            // nothing used to scale against, so use the measured time as-is rather than dividing by
            // zero (Inf/NaN).
            double estimatedEtlTimePercentage = storageUsed > 0
                    ? etlTimePercentage
                            * (double) (storageUsed + destinationMetrics.estimateStoragePVadded)
                            / (double) storageUsed
                    : etlTimePercentage;
            destinationMetrics.estimateETLtimePercentageAfterPVadded = estimatedEtlTimePercentage;

            if (estimatedEtlTimePercentage > percentageLimitation) {
                cpMetrics.setAvailable(false);
                configlogger.error("There is not enough time left for ETL to write " + pvName + " into "
                        + destinationMetrics.identity + ". Estimated percentage time is " + estimatedEtlTimePercentage
                        + " while the percentage limitation is " + percentageLimitation);
            }
        }
    }

    /**
     * Pick a uniformly random appliance from the given list, or {@code null} if the list is empty.
     * The {@link Random} is a parameter so tests can make the choice deterministic.
     */
    static ApplianceInfo randomAppliance(List<ApplianceInfo> appliances, Random random) {
        if (appliances.isEmpty()) {
            return null;
        }
        return appliances.get(random.nextInt(appliances.size()));
    }
}
