/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;
import org.epics.archiverappliance.retrieval.RetrievalState;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
                ClusterTopology {

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

    public static record CachedPVCounts(int totalPVCount, int pausedPVCount) implements Serializable {}

    /**
     * Get all the appliances in this cluster.
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     * @return ApplianceInfo &emsp;
     */
    public Iterable<ApplianceInfo> getAppliancesInCluster();

    /**
     * Get the appliance information for this appliance.
     * @return ApplianceInfo  &emsp;
     */
    public ApplianceInfo getMyApplianceInfo();

    /**
     * Given an identity of an appliance, return the appliance info for that appliance
     * @param identity The appliance identify
     * @return ApplianceInfo &emsp;
     */
    public ApplianceInfo getAppliance(String identity);

    /**
     * To prevent split brain side-effects, we support cetain BPL only when all the member of the cluster have finished loading their PVs into the cluster.
     * This consists of two checks
     * 1) Make sure all appliances listed in appliances.xml have started up and are part of the cluster
     * 2) All appliances in the cluster have registered their PVs with the cluster.
     *
     * Previously, we'd allow appliances.xml to have more appliances that are actually present in the cluster.
     * However; this is becoming increasingly hard to support.
     * We've had to tighten this to avoid split brain issues which can happen when the networking between instances fails.
     *
     * @return boolean &emsp;
     */
    public boolean hasClusterFinishedInitialization();

    /**
     * Get an exhaustive list of all the PVs this cluster of appliances knows about
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     * @return String AllPVs &emsp;
     */
    public Collection<String> getAllPVs();

    /**
     * For automated PV submission, IOC engineers could add .VAL, fields, aliases etc.
     * This method attempts to return all possible PV's that the archiver could know about.
     * This is a lot of names; so we take in a consumer that potentially streams a name out as quickly as possible.
     * @param func A consumer of pvNames
     */
    public void getAllExpandedNames(Consumer<String> func);

    /**
     * Given a PV, get us the appliance that is responsible for archiving it.
     * Note that this may be null as the assignment of PV's to appliances can take some time.
     * @param pvName The name of PV.
     * @return ApplianceInfo &emsp;
     */
    public ApplianceInfo getApplianceForPV(String pvName);

    /**
     * Get all PVs being archived by this appliance.
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     * @param info ApplianceInfo
     * @return string All PVs being archiveed by this appliance
     */
    public Set<String> getPVsForAppliance(ApplianceInfo info);

    /**
     * Get all the PVs for this appliance.
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     * @return String All PVs being archiveed for this appliance
     */
    public Set<String> getPVsForThisAppliance();

    /*
     * For performance reasons, we cache the total PV count and the paused PV count for this appliance.
     */
    public CachedPVCounts getCachedPVCountsForThisAppliance();

    /**
     * Prepare for batch jobs by breaking down a list of PV's into a Map that maps appliance identity to a list of PV's being archived on that appliance.
     * @return
     */
    public Map<String, List<String>> breakDownPVsByAppliance(List<String> pvNames);

    /**
     * Get the pvNames for this appliance matching the given regex.
     * @param nameToMatch  &emsp;
     * @return string PVsForApplianceMatchingRegex   &emsp;
     */
    public Set<String> getPVsForApplianceMatchingRegex(String nameToMatch);

    /**
     * Make changes in the config service to register this PV to an appliance
     * @param pvName The name of PV.
     * @param applianceInfo ApplianceInfo
     * @param registrationType If reassigning; then an AlreadyRegisteredException is not raised
     * @throws AlreadyRegisteredException  &emsp;
     */
    public void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo, PVRegistrationType registrationType)
            throws AlreadyRegisteredException;

    /*
     * Make changes in the config service to register this PV to an appliance
     * @param pvName The name of PV.
     * @param applianceInfo ApplianceInfo
     * @throws AlreadyRegisteredException  &emsp;
     */
    public default void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo)
            throws AlreadyRegisteredException {
        this.registerPVToAppliance(pvName, applianceInfo, PVRegistrationType.ARCHIVNG);
    }

    /**
     * Facilitates various optimizations for BPL that uses appliance wide information by caching and maintaining this information on a per appliance basis
     *
     * @param applianceInfo ApplianceInfo
     * @return ApplianceAggregateInfo  &emsp;
     * @throws IOException  &emsp;
     */
    public ApplianceAggregateInfo getAggregatedApplianceInfo(ApplianceInfo applianceInfo) throws IOException;

    /**
     * The workflow for requesting a PV to be archived consists of multiple steps
     * This method adds a PV to the persisted list of PVs that are currently engaged in this workflow in addition to any user specified overrides
     * @param pvName The name of PV.
     * @param userSpecifiedSamplingParams - Use a null contructor for userSpecifiedSamplingParams if no override specified.
     */
    public void addToArchiveRequests(String pvName, UserSpecifiedSamplingParams userSpecifiedSamplingParams);

    /**
     * Update the archive request (mostly with aliases) if and only if we have this in our persistence.
     * @param pvName  The name of PV.
     * @param userSpecifiedSamplingParams  &emsp;
     */
    public void updateArchiveRequest(String pvName, UserSpecifiedSamplingParams userSpecifiedSamplingParams);

    /**
     * Gets a list of PVs that are currently engaged in the archive PV workflow
     * @return String ArchiveRequestsCurrentlyInWorkflow  &emsp;
     */
    public Set<String> getArchiveRequestsCurrentlyInWorkflow();

    /**
     * Is this pv in the archive request workflow.
     * @param pvname The name of PV.
     * @return boolean True or False
     */
    public boolean doesPVHaveArchiveRequestInWorkflow(String pvname);

    /**
     * In clustered environments, to give capacity planning a chance to work correctly, we want to kick off the archive PV workflow only after all the machines have started.
     * This is an approximation for that metric; though not a very satisfactory approximation.
     * TODO -- Think thru implications of making the appliances.xml strict...
     * @return - Initial delay in seconds.
     */
    public int getInitialDelayBeforeStartingArchiveRequestWorkflow();

    /**
     * Returns any user specified parameters for the archive request.
     * @param pvName  The name of PV.
     * @return UserSpecifiedSamplingParams  &emsp;
     */
    public UserSpecifiedSamplingParams getUserSpecifiedSamplingParams(String pvName);

    /**
     * Mark this pv as having it archive pv request completed and pull this request out of persistent store
     * Can be used in the case of aborting a PV archive request as well
     * @param pvName  The name of PV.
     */
    public void archiveRequestWorkflowCompleted(String pvName);
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

    /**
     * Get a set of PVs that have been paused in this appliance.
     * @return String  &emsp;
     */
    public Set<String> getPausedPVsInThisAppliance();
}
