/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.bpl.reports;

import static org.epics.archiverappliance.retrieval.RetrievalMetrics.calculateSummedMetrics;

import org.epics.archiverappliance.common.reports.Metrics;
import org.epics.archiverappliance.config.ApplianceLifecycle;

import java.util.Map;

/**
 * Summary metrics for retrieval for an alliance.
 * @author mshankar
 *
 */
public class ApplianceMetrics implements Metrics {

    private final ApplianceLifecycle configService;

    public ApplianceMetrics(ApplianceLifecycle configService) {
        this.configService = configService;
    }

    @Override
    public ApplianceLifecycle configService() {
        return configService;
    }

    @Override
    public Map<String, String> metrics(ApplianceLifecycle configService) {
        return calculateSummedMetrics(configService).getMetrics();
    }
}
