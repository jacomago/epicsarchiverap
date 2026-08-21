package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.engine.epics.EngineMetrics;
import org.epics.archiverappliance.engine.pv.EngineContext;

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
        return EngineMetrics.computeEngineMetrics(EngineContext.of(configService))
                .details(configService);
    }
}
