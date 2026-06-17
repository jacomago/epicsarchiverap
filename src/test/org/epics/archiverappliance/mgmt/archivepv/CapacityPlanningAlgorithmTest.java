/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *****
 */
package org.epics.archiverappliance.mgmt.archivepv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.ETLMetrics;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unit tests for the pure capacity-planning decision logic in {@link CapacityPlanningAlgorithm} (and
 * the random fallback helper in {@link CapacityPlanningBPL}). These build metrics in memory and do
 * not touch the network.
 */
class CapacityPlanningAlgorithmTest {

    private static final float SECONDS_TO_BUFFER = 10f;
    private static final float LIMIT = 80f;

    // --- helpers ---------------------------------------------------------------------------------

    private static ApplianceInfo appliance(String identity) {
        return new ApplianceInfo(identity, "mgmt", "engine", "retrieval", "etl", "localhost:16670", "dataRetrieval");
    }

    private static ETLMetrics etl(String identity, long totalSpace, long available, double etlTimeTaken) {
        return new ETLMetrics(identity, totalSpace, available, etlTimeTaken);
    }

    private static CapacityPlanningData cpData(
            String identity, float currentRate, float totalChannelIOSeconds, ETLMetrics... etlMetrics) {
        ConcurrentHashMap<String, ETLMetrics> map = new ConcurrentHashMap<>();
        for (ETLMetrics m : etlMetrics) {
            map.put(m.identity, m);
        }
        return new CapacityPlanningData(identity, currentRate, totalChannelIOSeconds, map);
    }

    private static ApplianceAggregateInfo aggregate(double storageRate, Map<String, Long> impact) {
        ApplianceAggregateInfo info = new ApplianceAggregateInfo();
        info.setTotalStorageRate(storageRate);
        info.getTotalStorageImpact().putAll(impact);
        return info;
    }

    // --- pickByMaxFactor -------------------------------------------------------------------------

    @Test
    void writerFactorPicksApplianceWithLowestWriterPercentage() {
        ApplianceInfo a = appliance("a");
        ApplianceInfo b = appliance("b");
        CapacityPlanningData aData = cpData("a", 0, 0);
        CapacityPlanningData bData = cpData("b", 0, 0);
        aData.setAvailable(true);
        bData.setAvailable(true);
        aData.setPercentageTimeForWriter(10);
        bData.setPercentageTimeForWriter(5);
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, aData);
        appliances.put(b, bData);

        Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.pickByMaxFactor(
                appliances, List.of(new NormalizationFactor(CapacityPlanningAlgorithm.WRITER_FACTOR, 50)));

        assertEquals(b, chosen.orElseThrow());
    }

    @Test
    void etlFactorPicksApplianceWithLowestEtlPercentage() {
        ApplianceInfo a = appliance("a");
        ApplianceInfo b = appliance("b");
        ETLMetrics aEtl = etl("STS", 1000, 600, 10);
        ETLMetrics bEtl = etl("STS", 1000, 600, 10);
        aEtl.estimateETLtimePercentageAfterPVadded = 30;
        bEtl.estimateETLtimePercentageAfterPVadded = 20;
        CapacityPlanningData aData = cpData("a", 0, 0, aEtl);
        CapacityPlanningData bData = cpData("b", 0, 0, bEtl);
        aData.setAvailable(true);
        bData.setAvailable(true);
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, aData);
        appliances.put(b, bData);

        Optional<ApplianceInfo> chosen =
                CapacityPlanningAlgorithm.pickByMaxFactor(appliances, List.of(new NormalizationFactor("STS", 60)));

        assertEquals(b, chosen.orElseThrow());
    }

    // --- computeNormalizationFactors -------------------------------------------------------------

    @Test
    void normalizationFactorsAverageWriterAndSkipUnavailable() {
        ETLMetrics aEtl = etl("STS", 1000, 600, 10);
        ETLMetrics bEtl = etl("STS", 1000, 600, 10);
        aEtl.estimateETLtimePercentageAfterPVadded = 30;
        bEtl.estimateETLtimePercentageAfterPVadded = 40;
        CapacityPlanningData aData = cpData("a", 0, 0, aEtl);
        CapacityPlanningData bData = cpData("b", 0, 0, bEtl);
        CapacityPlanningData cData = cpData("c", 0, 0, etl("STS", 1000, 600, 10));
        aData.setAvailable(true);
        bData.setAvailable(true);
        cData.setAvailable(false); // excluded
        aData.setPercentageTimeForWriter(10);
        bData.setPercentageTimeForWriter(20);
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(appliance("a"), aData);
        appliances.put(appliance("b"), bData);
        appliances.put(appliance("c"), cData);

        List<NormalizationFactor> factors =
                CapacityPlanningAlgorithm.computeNormalizationFactors(appliances, Map.of("STS", 1));

        Map<String, Float> byName = new LinkedHashMap<>();
        for (NormalizationFactor f : factors) {
            byName.put(f.getIdentify(), f.getPercentage());
        }
        assertEquals(15f, byName.get(CapacityPlanningAlgorithm.WRITER_FACTOR)); // (10 + 20) / 2 available
        assertEquals(35f, byName.get("STS")); // (30 + 40) / 2 available
    }

    @Test
    void normalizationFactorsIncludeEachDestination() {
        ETLMetrics aSts = etl("STS", 1000, 600, 10);
        ETLMetrics aMts = etl("MTS", 1000, 600, 10);
        ETLMetrics bSts = etl("STS", 1000, 600, 10);
        ETLMetrics bMts = etl("MTS", 1000, 600, 10);
        aSts.estimateETLtimePercentageAfterPVadded = 20;
        aMts.estimateETLtimePercentageAfterPVadded = 30;
        bSts.estimateETLtimePercentageAfterPVadded = 40;
        bMts.estimateETLtimePercentageAfterPVadded = 10;
        CapacityPlanningData aData = cpData("a", 0, 0, aSts, aMts);
        CapacityPlanningData bData = cpData("b", 0, 0, bSts, bMts);
        aData.setAvailable(true);
        bData.setAvailable(true);
        aData.setPercentageTimeForWriter(10);
        bData.setPercentageTimeForWriter(20);
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(appliance("a"), aData);
        appliances.put(appliance("b"), bData);

        List<NormalizationFactor> factors =
                CapacityPlanningAlgorithm.computeNormalizationFactors(appliances, Map.of("STS", 1, "MTS", 1));

        Map<String, Float> byName = new LinkedHashMap<>();
        for (NormalizationFactor f : factors) {
            byName.put(f.getIdentify(), f.getPercentage());
        }
        assertEquals(3, byName.size());
        assertEquals(15f, byName.get(CapacityPlanningAlgorithm.WRITER_FACTOR)); // (10 + 20) / 2
        assertEquals(30f, byName.get("STS")); // (20 + 40) / 2
        assertEquals(20f, byName.get("MTS")); // (30 + 10) / 2
    }

    // --- markAvailability ------------------------------------------------------------------------

    @Test
    void storageOverAvailableMarksUnavailable() {
        // estimated storage = aggregate impact (500) + pvStorageRate * partitionSeconds (50 * 1) = 550 > 100
        CapacityPlanningData data = cpData("a", 100, 1, etl("STS", 1000, 100, 5));
        ApplianceAggregateInfo agg = aggregate(10, Map.of("STS", 500L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 50f, SECONDS_TO_BUFFER, LIMIT);

        assertFalse(data.isAvailable());
    }

    @Test
    void writerOverLimitMarksUnavailable() {
        // engine write thread usage = totalChannelIOSeconds * 100 / secondsToBuffer = 10 * 100 / 10 = 100 > 80
        CapacityPlanningData data = cpData("a", 100, 10, etl("STS", 1000, 600, 5));
        ApplianceAggregateInfo agg = aggregate(10, Map.of("STS", 100L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertFalse(data.isAvailable());
    }

    @Test
    void withinLimitsStaysAvailableAndComputesEtlPercentage() {
        // estimateStoragePVadded = aggregate impact (200) + pvStorageRate * partitionSeconds (200 * 1) = 400
        // storageUsed = 1000 - 600 = 400; etlTimeTaken = 10
        // estimateETLtimePercentageAfterPVadded = 10 * (400 + 400) / 400 = 20
        ETLMetrics sts = etl("STS", 1000, 600, 10);
        CapacityPlanningData data = cpData("a", 100, 1, sts);
        ApplianceAggregateInfo agg = aggregate(20, Map.of("STS", 200L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 200f, SECONDS_TO_BUFFER, LIMIT);

        assertTrue(data.isAvailable());
        assertEquals(400L, sts.estimateStoragePVadded);
        assertEquals(20.0, sts.estimateETLtimePercentageAfterPVadded, 1e-9);
    }

    @Test
    void etlTimeOverLimitMarksUnavailable() {
        // etlTimeTaken (90) already exceeds the limit before the PV is even added
        CapacityPlanningData data = cpData("a", 100, 1, etl("STS", 1000, 600, 90));
        ApplianceAggregateInfo agg = aggregate(0, Map.of("STS", 100L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertFalse(data.isAvailable());
    }

    @Test
    void estimatedEtlTimeOverLimitMarksUnavailable() {
        // etlTimeTaken (50) is under the limit, but adding the PV pushes the projected ETL time over:
        // storageUsed = 200 - 100 = 100; estimateStoragePVadded = 0 + 100 * 1 = 100 (= available, so storage is ok)
        // estimateETLtimePercentageAfterPVadded = 50 * (100 + 100) / 100 = 100 > 80
        CapacityPlanningData data = cpData("a", 100, 1, etl("STS", 200, 100, 50));
        ApplianceAggregateInfo agg = aggregate(0, Map.of("STS", 0L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 100f, SECONDS_TO_BUFFER, LIMIT);

        assertFalse(data.isAvailable());
    }

    @Test
    void storageEqualToAvailableStaysAvailable() {
        // estimated storage = aggregate impact (500) + 0 = 500, exactly the available storage (not over)
        ETLMetrics sts = etl("STS", 1000, 500, 5);
        CapacityPlanningData data = cpData("a", 100, 1, sts);
        ApplianceAggregateInfo agg = aggregate(0, Map.of("STS", 500L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertTrue(data.isAvailable());
    }

    @Test
    void storageIsNotCheckedForDestinationsThePvDoesNotWriteTo() {
        // The PV writes only to STS. LTS has almost no free space and a large pending impact, but since
        // the PV does not write to LTS its storage is never checked, so the appliance stays available.
        CapacityPlanningData data = cpData("a", 100, 1, etl("STS", 1000, 600, 5), etl("LTS", 1000, 1, 5));
        ApplianceAggregateInfo agg = aggregate(0, Map.of("STS", 100L, "LTS", 999_999L));

        CapacityPlanningAlgorithm.markAvailability("pv", data, agg, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertTrue(data.isAvailable());
    }

    // --- decide ----------------------------------------------------------------------------------

    @Test
    void decideExcludesApplianceWithoutEtlMetrics() {
        ApplianceInfo a = appliance("a");
        ApplianceInfo b = appliance("b");
        CapacityPlanningData aData = cpData("a", 100, 1, etl("STS", 1000, 600, 10));
        CapacityPlanningData bData = cpData("b", 50, 1); // no ETL metrics -> excluded
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, aData);
        appliances.put(b, bData);
        Map<ApplianceInfo, ApplianceAggregateInfo> diffs = new LinkedHashMap<>();
        diffs.put(a, aggregate(20, Map.of("STS", 200L)));
        diffs.put(b, aggregate(20, Map.of()));

        Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                "pv", appliances, diffs, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertEquals(a, chosen.orElseThrow());
        assertFalse(bData.isAvailable());
    }

    @Test
    void decideColdStartPicksLowestDataRateWhenNoEtlMetrics() {
        ApplianceInfo a = appliance("a");
        ApplianceInfo b = appliance("b");
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, cpData("a", 100, 1)); // no ETL metrics
        appliances.put(b, cpData("b", 50, 1)); // no ETL metrics, lower rate
        Map<ApplianceInfo, ApplianceAggregateInfo> diffs = new LinkedHashMap<>();
        diffs.put(a, aggregate(0, Map.of()));
        diffs.put(b, aggregate(0, Map.of()));

        Optional<ApplianceInfo> chosen =
                CapacityPlanningAlgorithm.decide("pv", appliances, diffs, Map.of(), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertEquals(b, chosen.orElseThrow());
    }

    @Test
    void decideReturnsEmptyWhenNoApplianceAvailable() {
        ApplianceInfo a = appliance("a");
        CapacityPlanningData aData = cpData("a", 100, 1, etl("STS", 1000, 100, 5)); // storage will be exceeded
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, aData);
        Map<ApplianceInfo, ApplianceAggregateInfo> diffs = new LinkedHashMap<>();
        diffs.put(a, aggregate(10, Map.of("STS", 500L))); // 500 > 100 available

        Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                "pv", appliances, diffs, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertTrue(chosen.isEmpty());
    }

    @Test
    void decideWithSingleAvailableApplianceReturnsIt() {
        ApplianceInfo a = appliance("a");
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, cpData("a", 100, 1, etl("STS", 1000, 600, 5)));
        Map<ApplianceInfo, ApplianceAggregateInfo> diffs = new LinkedHashMap<>();
        diffs.put(a, aggregate(0, Map.of("STS", 100L)));

        Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                "pv", appliances, diffs, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertEquals(a, chosen.orElseThrow());
    }

    @Test
    void decideFullFlowPicksApplianceWithLowestWriterWhenWriterDominates() {
        // Both appliances healthy. Writer percentages (10 vs 20) dominate the tiny ETL factor (4),
        // so the appliance with the lower writer percentage (a) is chosen.
        ApplianceInfo a = appliance("a");
        ApplianceInfo b = appliance("b");
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, cpData("a", 100, 1, etl("STS", 1000, 600, 4))); // writer usage 10%
        appliances.put(b, cpData("b", 100, 2, etl("STS", 1000, 600, 4))); // writer usage 20%
        Map<ApplianceInfo, ApplianceAggregateInfo> diffs = new LinkedHashMap<>();
        diffs.put(a, aggregate(0, Map.of("STS", 0L)));
        diffs.put(b, aggregate(0, Map.of("STS", 0L)));

        Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                "pv", appliances, diffs, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertEquals(a, chosen.orElseThrow());
    }

    @Test
    void decideFullFlowPicksApplianceWithLowestEtlWhenEtlDominates() {
        // Equal writer usage (10%), but the ETL destination dominates; the appliance with the lower
        // ETL time (a, etlTimeTaken 40 vs b 50) is chosen.
        ApplianceInfo a = appliance("a");
        ApplianceInfo b = appliance("b");
        Map<ApplianceInfo, CapacityPlanningData> appliances = new LinkedHashMap<>();
        appliances.put(a, cpData("a", 100, 1, etl("STS", 1000, 200, 40)));
        appliances.put(b, cpData("b", 100, 1, etl("STS", 1000, 200, 50)));
        Map<ApplianceInfo, ApplianceAggregateInfo> diffs = new LinkedHashMap<>();
        diffs.put(a, aggregate(0, Map.of("STS", 0L)));
        diffs.put(b, aggregate(0, Map.of("STS", 0L)));

        Optional<ApplianceInfo> chosen = CapacityPlanningAlgorithm.decide(
                "pv", appliances, diffs, Map.of("STS", 1), 0f, SECONDS_TO_BUFFER, LIMIT);

        assertEquals(a, chosen.orElseThrow());
    }

    // --- estimatedStorageForDestination ----------------------------------------------------------

    @Test
    void estimatedStorageAddsPvContributionToAggregateImpact() {
        assertEquals(
                300L, CapacityPlanningAlgorithm.estimatedStorageForDestination(Map.of("STS", 200L), "STS", 50f, 2));
    }

    @Test
    void estimatedStorageTreatsMissingDestinationAsZeroImpact() {
        assertEquals(100L, CapacityPlanningAlgorithm.estimatedStorageForDestination(Map.of(), "STS", 50f, 2));
    }

    // --- randomAppliance -------------------------------------------------------------------------

    @Test
    void randomApplianceIsDeterministicForASeed() {
        List<ApplianceInfo> appliances = List.of(appliance("a"), appliance("b"), appliance("c"));
        ApplianceInfo expected = appliances.get(new Random(1234).nextInt(appliances.size()));

        assertEquals(expected, CapacityPlanningBPL.randomAppliance(appliances, new Random(1234)));
        assertTrue(appliances.contains(CapacityPlanningBPL.randomAppliance(appliances, new Random())));
    }

    @Test
    void randomApplianceReturnsNullForEmptyList() {
        assertNull(CapacityPlanningBPL.randomAppliance(List.of(), new Random()));
    }
}
