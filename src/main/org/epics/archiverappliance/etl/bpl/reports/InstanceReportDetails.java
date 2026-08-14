package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.etl.common.ETLMetrics;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;

import java.util.LinkedList;
import java.util.Map;

public class InstanceReportDetails implements MetricsDetails {

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ApplianceLifecycle configService) {
        ETLMetrics etlMetrics = PBThreeTierETLPVLookup.of(configService).getApplianceMetrics();
        return etlMetrics.details(configService);
    }
}
