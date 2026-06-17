/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *****
 */
package org.epics.archiverappliance.mgmt.archivepv;

import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.CPStaticData;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Gathers the per-appliance data a capacity planning decision needs, and is the seam between the
 * different metric tiers.
 *
 * <p>Today it returns the measured metrics (engine/ETL, cached for an hour by {@link
 * CapacityPlanningData}) together with the freshly fetched aggregate differences. Later tiers — a
 * short background poll of free storage and an event-driven per-appliance load view — will plug in
 * here without changing the decision logic in {@link CapacityPlanningAlgorithm}.
 */
class CapacityPlanningMetricsProvider {
    private CapacityPlanningMetricsProvider() {}

    /**
     * A consistent set of inputs for one planning decision.
     *
     * @param metricsTimestamp when the measured metrics were last fetched; used to reset the
     *     orchestrator's in-flight counters when the metrics are refetched.
     * @param appliances measured capacity planning data per appliance.
     * @param aggregateDifferences the appliance aggregate info difference since the metrics were last
     *     fetched, per appliance.
     */
    record Snapshot(
            Instant metricsTimestamp,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences) {}

    static Snapshot gather(ConfigService configService) throws IOException {
        CPStaticData measured = CapacityPlanningData.getMetricsForAppliances(configService);
        Map<ApplianceInfo, CapacityPlanningData> appliances = measured.cpApplianceMetrics;

        // Fetch the aggregate difference once per appliance so the decision itself does no I/O.
        Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences = new HashMap<>();
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            aggregateDifferences.put(
                    entry.getKey(), entry.getValue().getApplianceAggregateDifferenceFromLastFetch(configService));
        }
        return new Snapshot(measured.timeofData, appliances, aggregateDifferences);
    }
}
