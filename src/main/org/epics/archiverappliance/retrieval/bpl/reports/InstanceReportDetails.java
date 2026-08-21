package org.epics.archiverappliance.retrieval.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.retrieval.RetrievalMetrics;

import java.util.LinkedList;
import java.util.Map;

public class InstanceReportDetails implements MetricsDetails {

    private final ApplianceLifecycle configService;

    public InstanceReportDetails(ApplianceLifecycle configService) {
        this.configService = configService;
    }

    @Override
    public ApplianceLifecycle configService() {
        return configService;
    }

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ApplianceLifecycle configService) {
        return RetrievalMetrics.calculateSummedMetrics(configService).details(configService);
    }
}
