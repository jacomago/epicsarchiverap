/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.config.ArchServletContextListener;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.exception.ConfigException;

/**
 * The retrieval webapp's ServletContextListener; registered in the retrieval web.xml in place of
 * ArchServletContextListener. Builds the retrieval state once the config service is up.
 * @author mshankar
 *
 */
public class RetrievalServletContextListener extends ArchServletContextListener {

    @Override
    protected void createRuntimeState(ConfigService configService) throws ConfigException {
        RetrievalState.create(configService);
        if (RetrievalState.of(configService) == null) {
            throw new ConfigException("Unable to publish the retrieval state");
        }
    }
}
