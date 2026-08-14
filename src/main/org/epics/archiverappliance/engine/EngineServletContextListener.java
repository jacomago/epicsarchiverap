/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine;

import org.epics.archiverappliance.config.ArchServletContextListener;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.engine.pv.EngineContext;

/**
 * The engine webapp's ServletContextListener; registered in the engine web.xml in place of
 * ArchServletContextListener. Builds the engine context once the config service is up.
 * @author mshankar
 *
 */
public class EngineServletContextListener extends ArchServletContextListener {

    @Override
    protected void createRuntimeState(ConfigService configService) throws ConfigException {
        EngineContext.create(configService);
        if (EngineContext.of(configService) == null) {
            throw new ConfigException("Unable to publish the engine context");
        }
    }
}
