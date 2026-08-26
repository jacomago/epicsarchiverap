/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

/**
 * What {@link ClusterExecutor} hands a callback when it runs on a remote appliance.
 * <p>
 * A callback runs somewhere else in the cluster, so its caller cannot supply its concerns — the
 * executor does, and one set has to serve every callback. This names that set. A callback needing
 * less should say so: {@code EAABulkOperation} is generic in its parameter and the executor accepts
 * any type this view is assignable to.
 */
public interface ClusterCallbackView extends ApplianceLifecycle, ClusterTopology, PVDirectory, PVTypeInfoStore {}
