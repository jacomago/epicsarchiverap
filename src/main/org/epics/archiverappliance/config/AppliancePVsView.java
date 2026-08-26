/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

/**
 * Doing work across the PVs this appliance owns needs two things together: the directory, to know
 * which PVs those are, and the lifecycle handle, which is what locates this appliance's runtime
 * state. This composes them and declares no methods of its own; Java has no intersection type for a
 * parameter, and the alternative is handing the caller the whole config service.
 * <ul>
 * <li>{@link ApplianceLifecycle} — the key that resolves this webapp's runtime state.</li>
 * <li>{@link PVDirectory} — which PVs this appliance owns.</li>
 * </ul>
 * The ETL side is the recurring consumer: walking the local PVs and asking each one's ETL stages for
 * its metrics is the shape of both the storage reports and the ETL run itself. A caller that only
 * reads the directory, or only needs the runtime handle, wants that concern named on its own.
 */
public interface AppliancePVsView extends ApplianceLifecycle, PVDirectory {}
