/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import org.epics.archiverappliance.engine.pv.EngineContext;

/**
 * Config service sub-interface for the engine webapp.
 * Extends {@link CoreConfigService} with the engine runtime-state getter.
 */
public interface EngineConfigService extends CoreConfigService {
    /**
     * Return the runtime state for the engine.
     * @return EngineContext &emsp;
     */
    EngineContext getEngineContext();
}
