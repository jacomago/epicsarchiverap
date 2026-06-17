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

    /** The normalization factor identity used for the engine write thread. */
    static final String WRITER_FACTOR = "writer";
    /** Sentinel identity used while searching for the maximum factor. */
    private static final String NO_FACTOR = "tempFactor";

    private CapacityPlanningAlgorithm() {}

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
     * @return the chosen appliance, or empty if no appliance could be decided.
     */
    static Optional<ApplianceInfo> decide(
            String pvName,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences,
            Map<String, Integer> destinationPartitionSeconds,
            float pvStorageRate,
            float secondsToBuffer,
            float percentageLimitation) {

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

        List<NormalizationFactor> factors = computeNormalizationFactors(appliances, destinationPartitionSeconds);
        if (factors.isEmpty()) {
            configlogger.error("There is no appliance available for " + pvName);
            return Optional.empty();
        }

        return pickByMaxFactor(appliances, factors);
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
        float percentageForWriter =
                currentUsedWriterPercentage * (totalDataRateForPvAdded + totalDataRate) / (totalDataRate);
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

            double estimatedEtlTimePercentage = etlTimePercentage
                    * (double) (storageUsed + destinationMetrics.estimateStoragePVadded)
                    / (double) storageUsed;
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
     * Build the normalization factors used to decide which resource is most constrained: one for the
     * engine writer and one per ETL destination, each the average percentage across the available
     * appliances. Returns an empty list when no appliance is available.
     */
    static List<NormalizationFactor> computeNormalizationFactors(
            Map<ApplianceInfo, CapacityPlanningData> appliances, Map<String, Integer> destinationPartitionSeconds) {
        float totalWriterPercentage = 0;
        int availableAppliancesNum = 0;
        Map<String, Double> etlPercentageByDestination = new HashMap<>();

        for (CapacityPlanningData cpMetrics : appliances.values()) {
            if (!cpMetrics.isAvailable()) {
                continue;
            }
            availableAppliancesNum++;
            totalWriterPercentage += cpMetrics.getPercentageTimeForWriter();
            Map<String, ETLMetrics> etlMetrics = cpMetrics.getEtlMetrics();
            for (String destinationName : destinationPartitionSeconds.keySet()) {
                ETLMetrics destinationMetrics = etlMetrics.get(destinationName);
                if (destinationMetrics != null) {
                    etlPercentageByDestination.merge(
                            destinationName, destinationMetrics.estimateETLtimePercentageAfterPVadded, Double::sum);
                }
            }
        }

        List<NormalizationFactor> factors = new ArrayList<>();
        if (availableAppliancesNum == 0) {
            return factors;
        }
        factors.add(new NormalizationFactor(WRITER_FACTOR, totalWriterPercentage / availableAppliancesNum));
        for (Map.Entry<String, Double> etlEntry : etlPercentageByDestination.entrySet()) {
            factors.add(
                    new NormalizationFactor(etlEntry.getKey(), (float) (etlEntry.getValue() / availableAppliancesNum)));
        }
        return factors;
    }

    /**
     * Pick the most constrained factor (highest percentage), then among the available appliances pick
     * the one with the lowest usage of that factor. Returns empty if no factor or appliance qualifies.
     */
    static Optional<ApplianceInfo> pickByMaxFactor(
            Map<ApplianceInfo, CapacityPlanningData> appliances, List<NormalizationFactor> factors) {
        // Only factors with a non-negative percentage qualify; ties resolve to the last one seen.
        NormalizationFactor maxFactor = new NormalizationFactor(NO_FACTOR, 0);
        for (NormalizationFactor factor : factors) {
            if (factor.getPercentage() >= maxFactor.getPercentage()) {
                maxFactor = factor;
            }
        }
        if (maxFactor.getIdentify().equals(NO_FACTOR)) {
            return Optional.empty();
        }
        logger.debug("the max (percentage) is :" + maxFactor.getIdentify() + ",value:" + maxFactor.getPercentage());

        String maxFactorIdentity = maxFactor.getIdentify();
        ApplianceInfo chosen = null;
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            CapacityPlanningData cpMetrics = entry.getValue();
            ApplianceInfo applianceInfo = entry.getKey();
            if (!cpMetrics.isAvailable()) {
                continue;
            }
            if (chosen == null) {
                chosen = applianceInfo;
                continue;
            }
            if (maxFactorIdentity.equals(WRITER_FACTOR)) {
                if (cpMetrics.getPercentageTimeForWriter()
                        < appliances.get(chosen).getPercentageTimeForWriter()) {
                    chosen = applianceInfo;
                }
            } else {
                double chosenEtl = appliances
                        .get(chosen)
                        .getEtlMetrics()
                        .get(maxFactorIdentity)
                        .estimateETLtimePercentageAfterPVadded;
                double candidateEtl =
                        cpMetrics.getEtlMetrics().get(maxFactorIdentity).estimateETLtimePercentageAfterPVadded;
                if (candidateEtl < chosenEtl) {
                    chosen = applianceInfo;
                }
            }
        }
        return Optional.ofNullable(chosen);
    }
}
