/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt;

import org.epics.archiverappliance.config.ArchServletContextListener;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.exception.ConfigException;

/**
 * The mgmt webapp's ServletContextListener; registered in the mgmt web.xml in place of
 * ArchServletContextListener. Builds the mgmt runtime state once the config service is up.
 * @author mshankar
 *
 */
public class MgmtServletContextListener extends ArchServletContextListener {

    @Override
    protected void createRuntimeState(ConfigService configService) throws ConfigException {
        MgmtRuntimeState.create(configService);
        if (MgmtRuntimeState.of(configService) == null) {
            throw new ConfigException("Unable to publish the mgmt runtime state");
        }
    }
}
