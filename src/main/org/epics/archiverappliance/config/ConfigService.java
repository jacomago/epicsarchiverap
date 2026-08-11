/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;
import org.epics.archiverappliance.retrieval.RetrievalState;

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
                ArchiveRequestWorkflow {

    /**
     * This is the environment variable that identifies the site (LCLS, LCLSII, slacdev, NSLSII etc) to be used when generating the war files.
     * This is primarily a build-time property; the build.xml has various site specific hooks which let you change the appliances.xml, policies, images etc on a per site basis.
     * The unit tests use the <code>tests</code> site which is also the default site if this environment variable is not specified.
     * Files for a site are stored in the sitespecific/&lt;site&gt; folder.
     */
    public static final String ARCHAPPL_SITEID = "ARCHAPPL_SITEID";

    /**
     * This is an optional environment/system.property that is used to identity the persistence layer
     * If this is not set, we initialize MySQLPersistence as the persistence layer; so in production environments, you can leave this unset/blank
     * Set this to the class name of the class implementing {@link ConfigPersistence ConfigPersistence}
     * The unit tests however will set this to use InMemoryPersistence, which is a dummy persistence layer.
     */
    public static final String ARCHAPPL_PERSISTENCE_LAYER = "ARCHAPPL_PERSISTENCE_LAYER";

    /**
     * Returns the runtime state for the retrieval app
     * @return RetrievalState &emsp;
     */
    public RetrievalState getRetrievalRuntimeState();

    /**
     * Return the runtime state for ETL.
     * This may eventually be moved to a RunTime class but that would still start from the configservice.
     * @return PBThreeTierETLPVLookup &emsp;
     */
    public PBThreeTierETLPVLookup getETLLookup();

    /**
     * Return the runtime state for the engine.
     * @return EngineContext &emsp;
     */
    public EngineContext getEngineContext();

    /**
     * Return the runtime state for the mgmt webapp.
     * @return  MgmtRuntimeStat &emsp;
     */
    public MgmtRuntimeState getMgmtRuntimeState();

    // Various reporting helper functions start here

}
