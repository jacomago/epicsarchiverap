/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import org.epics.archiverappliance.config.ArchServletContextListener;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;

/**
 * The ETL webapp's ServletContextListener; registered in the ETL web.xml in place of
 * ArchServletContextListener. Builds the ETL lookup once the config service is up and puts its
 * startup into the config service's post startup sequence, where it has to run after the cluster
 * has come up rather than at context initialization.
 * @author mshankar
 *
 */
public class ETLServletContextListener extends ArchServletContextListener {

    @Override
    protected void createRuntimeState(ConfigService configService) throws ConfigException {
        PBThreeTierETLPVLookup etlPVLookup = PBThreeTierETLPVLookup.create(configService);
        if (PBThreeTierETLPVLookup.of(configService) == null) {
            throw new ConfigException("Unable to publish the ETL lookup");
        }
        configService.addPostStartupHook(etlPVLookup::postStartup);
    }
}
