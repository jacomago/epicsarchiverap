/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import org.epics.archiverappliance.retrieval.RetrievalState;

/**
 * Config service sub-interface for the retrieval webapp.
 * Extends {@link CoreConfigService} with the retrieval runtime-state getter.
 */
public interface RetrievalConfigService extends CoreConfigService {
    /**
     * Returns the runtime state for the retrieval app.
     * @return RetrievalState &emsp;
     */
    RetrievalState getRetrievalRuntimeState();
}
