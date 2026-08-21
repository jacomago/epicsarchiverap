/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

/**
 * Interface for appliance configuration.
 * One gets to a config service implementation thru dependency injection of one kind or the other.
 * In a servlet container, this is initialized by ArchServletContextListener (which is registered as part of the web.xml).
 * ArchServletContextListener is also used in the unit tests that involve tomcat.
 * Guice is a good option for this but it takes over the dispatch logic from tomcat and we'll need to investigate if that has any impact.
 * @author mshankar
 */
public interface ConfigService
        extends PVTypeInfoStore,
                AliasRegistry,
                ClusterExecutor,
                PolicyService,
                PVNamingConfig,
                ChannelArchiverConfig,
                FailoverConfig,
                InstallationProperties,
                ApplianceLifecycle,
                ClusterTopology,
                PVDirectory,
                ArchiveRequestWorkflow,
                StoragePluginConfigView,
                PVTypeInfoLookupView,
                AppliancePVsView,
                ClusterCallbackView,
                ProcessMetricsSource {

    // Various reporting helper functions start here

}
