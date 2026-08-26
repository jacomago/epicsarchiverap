/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import org.epics.archiverappliance.common.ProcessMetrics;

/**
 * JVM-level metrics for the webapp this config service belongs to.
 * <p>
 * The two reports that read these used to cast their parameter down to the implementation, on the
 * grounds that the aggregate interface should not carry process metrics. Splitting the aggregate
 * gives them somewhere to live that is neither the god object nor the implementation.
 */
public interface ProcessMetricsSource {
    /**
     * @return This webapp's process metrics.
     */
    ProcessMetrics getProcessMetrics();
}
