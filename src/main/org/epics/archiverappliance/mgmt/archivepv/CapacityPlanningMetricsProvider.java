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
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.CPStaticData;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.ETLMetrics;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Gathers the per-appliance data a capacity planning decision needs, and is the seam between the
 * different metric tiers.
 *
 * <p>The measured metrics (engine/ETL) are cached for an hour by {@link CapacityPlanningData}. The
 * faster-moving but cheap signals — free storage per store and the per-appliance aggregate
 * difference — are refreshed by a single background poller on a short interval, so the decision path
 * does no per-PV HTTP fan-out. {@link #gather} reads the polled caches (fetching synchronously only
 * the first time an appliance is seen) and overlays the fresh free storage onto the cached snapshot.
 */
class CapacityPlanningMetricsProvider {
    private static final Logger logger = LogManager.getLogger(CapacityPlanningMetricsProvider.class.getName());

    /** Installation property overriding how often (seconds) the fast-moving signals are polled. */
    static final String POLL_SECONDS_PROPERTY =
            "org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningMetricsProvider.pollSeconds";

    private static final int DEFAULT_POLL_SECONDS = 60;

    private static final CapacityPlanningMetricsProvider INSTANCE = new CapacityPlanningMetricsProvider();

    static CapacityPlanningMetricsProvider instance() {
        return INSTANCE;
    }

    private final AtomicBoolean pollerStarted = new AtomicBoolean(false);

    /** Latest polled available storage (bytes) per store, keyed by appliance identity then store. */
    private final ConcurrentHashMap<String, Map<String, Long>> freeStorageByApplianceIdentity =
            new ConcurrentHashMap<>();

    /** Latest polled aggregate difference since the measured metrics were fetched, by appliance identity. */
    private final ConcurrentHashMap<String, ApplianceAggregateInfo> aggregateDifferenceByApplianceIdentity =
            new ConcurrentHashMap<>();

    private CapacityPlanningMetricsProvider() {}

    /**
     * A consistent set of inputs for one planning decision.
     *
     * @param metricsTimestamp when the measured metrics were last fetched; used to reset the
     *     orchestrator's in-flight counters when the metrics are refetched.
     * @param appliances measured capacity planning data per appliance (free storage overlaid fresh).
     * @param aggregateDifferences the appliance aggregate info difference since the metrics were last
     *     fetched, per appliance.
     */
    record Snapshot(
            Instant metricsTimestamp,
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences) {}

    Snapshot gather(ConfigService configService) throws IOException {
        startPoller(configService);

        CPStaticData measured = CapacityPlanningData.getMetricsForAppliances(configService);
        Map<ApplianceInfo, CapacityPlanningData> appliances = measured.cpApplianceMetrics;

        Map<ApplianceInfo, ApplianceAggregateInfo> aggregateDifferences = new HashMap<>();
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            aggregateDifferences.put(
                    entry.getKey(), aggregateDifferenceFor(entry.getKey(), entry.getValue(), configService));
        }

        overlayFreeStorage(appliances, freeStorageByApplianceIdentity);
        return new Snapshot(measured.timeofData, appliances, aggregateDifferences);
    }

    /**
     * The background-polled aggregate difference for an appliance, fetched synchronously the first
     * time the appliance is seen (cold start or a newly added appliance) so the decision is never
     * left without it.
     */
    private ApplianceAggregateInfo aggregateDifferenceFor(
            ApplianceInfo applianceInfo, CapacityPlanningData cpData, ConfigService configService) throws IOException {
        ApplianceAggregateInfo polled = aggregateDifferenceByApplianceIdentity.get(applianceInfo.getIdentity());
        if (polled != null) {
            return polled;
        }
        ApplianceAggregateInfo fetched = cpData.getApplianceAggregateDifferenceFromLastFetch(configService);
        aggregateDifferenceByApplianceIdentity.put(applianceInfo.getIdentity(), fetched);
        return fetched;
    }

    /**
     * Overwrite each store's available storage with the freshest polled value (when one exists),
     * leaving the hour-cached total size and ETL/writer figures untouched.
     */
    static void overlayFreeStorage(
            Map<ApplianceInfo, CapacityPlanningData> appliances,
            Map<String, Map<String, Long>> freeStorageByApplianceIdentity) {
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : appliances.entrySet()) {
            Map<String, Long> freshFreeStorage =
                    freeStorageByApplianceIdentity.get(entry.getKey().getIdentity());
            if (freshFreeStorage == null) {
                continue;
            }
            for (ETLMetrics destinationMetrics :
                    entry.getValue().getEtlMetrics().values()) {
                Long available = freshFreeStorage.get(destinationMetrics.identity);
                if (available != null) {
                    destinationMetrics.etlStorageAvailable = available;
                }
            }
        }
    }

    private void startPoller(ConfigService configService) {
        if (pollerStarted.getAndSet(true)) {
            return;
        }
        int pollSeconds = Integer.parseInt(configService
                .getInstallationProperties()
                .getProperty(POLL_SECONDS_PROPERTY, Integer.toString(DEFAULT_POLL_SECONDS)));
        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "capacity-planning-metrics-poller");
            thread.setDaemon(true);
            return thread;
        });
        poller.scheduleWithFixedDelay(() -> poll(configService), 0, pollSeconds, TimeUnit.SECONDS);
        configService.addShutdownHook(poller::shutdownNow);
        logger.info("Started capacity planning metrics poller every " + pollSeconds + "s");
    }

    private void poll(ConfigService configService) {
        CPStaticData measured;
        try {
            measured = CapacityPlanningData.getMetricsForAppliances(configService);
        } catch (Exception e) {
            logger.warn("Could not refresh capacity planning metrics", e);
            return;
        }
        for (Map.Entry<ApplianceInfo, CapacityPlanningData> entry : measured.cpApplianceMetrics.entrySet()) {
            ApplianceInfo applianceInfo = entry.getKey();
            try {
                freeStorageByApplianceIdentity.put(
                        applianceInfo.getIdentity(), CapacityPlanningData.fetchAvailableStorage(applianceInfo));
            } catch (Exception e) {
                logger.warn("Could not poll free storage for appliance " + applianceInfo.getIdentity(), e);
            }
            try {
                aggregateDifferenceByApplianceIdentity.put(
                        applianceInfo.getIdentity(),
                        entry.getValue().getApplianceAggregateDifferenceFromLastFetch(configService));
            } catch (Exception e) {
                logger.warn("Could not poll aggregate difference for appliance " + applianceInfo.getIdentity(), e);
            }
        }
    }
}
