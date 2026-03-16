/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;
import org.epics.archiverappliance.mgmt.MgmtPostStartup;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;
import org.epics.archiverappliance.mgmt.NonMgmtPostStartup;
import org.epics.archiverappliance.mgmt.bpl.cahdlers.NamesHandler;
import org.epics.archiverappliance.mgmt.policy.ExecutePolicy;
import org.epics.archiverappliance.retrieval.RetrievalState;
import org.epics.archiverappliance.retrieval.channelarchiver.ChannelArchiverDataServerInfo;
import org.epics.archiverappliance.retrieval.channelarchiver.XMLRPCClient;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The default config service for the archiver appliance.
 * Extends {@link AbstractConfigService} with service-specific runtime state
 * (EngineContext, RetrievalState, PBThreeTierETLPVLookup, MgmtRuntimeState)
 * and policy evaluation via ExecutePolicy.
 *
 * @author mshankar
 */
public class DefaultConfigService extends AbstractConfigService implements ConfigService {

    private static final Logger logger = LogManager.getLogger(DefaultConfigService.class.getName());
    private static final Logger configlogger = LogManager.getLogger("config." + DefaultConfigService.class.getName());

    // Service-specific runtime state
    private EngineContext engineContext = null;
    private RetrievalState retrievalState = null;
    private PBThreeTierETLPVLookup etlPVLookup = null;
    private MgmtRuntimeState mgmtRuntime = null;

    // Use a Guava cache to store one and only one ExecutePolicy object that expires after some inactivity.
    // The side effect is that it may take this many minutes to update the policy that is cached.
    private final LoadingCache<String, ExecutePolicy> theExecutionPolicy = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .removalListener((RemovalListener<String, ExecutePolicy>)
                    arg -> arg.getValue().close())
            .build(new CacheLoader<String, ExecutePolicy>() {
                public ExecutePolicy load(String key) throws IOException {
                    logger.info("Updating the cached execute policy");
                    return new ExecutePolicy(DefaultConfigService.this);
                }
            });

    protected DefaultConfigService() {
        // Only the unit tests config service uses this constructor.
    }

    // =========================================================================
    // Hook method overrides
    // =========================================================================

    @Override
    protected void initializeServiceContext(String contextPath) {
        switch (contextPath) {
            case "/mgmt":
                warFile = WAR_FILE.MGMT;
                this.mgmtRuntime = new MgmtRuntimeState(this);
                break;
            case "/engine":
                warFile = WAR_FILE.ENGINE;
                this.engineContext = new EngineContext(this);
                break;
            case "/retrieval":
                warFile = WAR_FILE.RETRIEVAL;
                this.retrievalState = new RetrievalState(this);
                break;
            case "/etl":
                this.etlPVLookup = new PBThreeTierETLPVLookup(this);
                warFile = WAR_FILE.ETL;
                break;
            default:
                logger.error("We seem to have introduced a new component into the system " + contextPath);
        }
    }

    @Override
    protected void schedulePostStartupTasks() {
        if (this.warFile == WAR_FILE.MGMT) {
            logger.info("Scheduling webappReady's for the mgmt webapp ");
            MgmtPostStartup mgmtPostStartup = new MgmtPostStartup(this);
            ScheduledFuture<?> postStartupFuture =
                    startupExecutor.scheduleAtFixedRate(mgmtPostStartup, 10, 20, TimeUnit.SECONDS);
            mgmtPostStartup.setCancellingFuture(postStartupFuture);
        } else {
            logger.info("Scheduling webappReady's for the non-mgmt webapp " + this.warFile.toString());
            NonMgmtPostStartup nonMgmtPostStartup = new NonMgmtPostStartup(this, this.warFile.toString());
            ScheduledFuture<?> postStartupFuture =
                    startupExecutor.scheduleAtFixedRate(nonMgmtPostStartup, 10, 20, TimeUnit.SECONDS);
            nonMgmtPostStartup.setCancellingFuture(postStartupFuture);
        }
    }

    @Override
    protected void postStartupEngine() {
        this.startupExecutor.schedule(
                () -> {
                    try {
                        logger.debug(() -> "Starting up the engine's channels on startup.");
                        archivePVSonStartup();
                        logger.debug(() -> "Done starting up the engine's channels in startup.");
                    } catch (Throwable t) {
                        configlogger.fatal("Exception starting up the engine channels on startup", t);
                    }
                },
                1,
                TimeUnit.SECONDS);
    }

    @Override
    protected void postStartupEtl() {
        this.etlPVLookup.postStartup();
    }

    @Override
    protected void addInfoForPVToAggregate(ApplianceAggregateInfo aggregateInfo, String pvName, PVTypeInfo typeInfo) {
        // Call the base to handle totals
        super.addInfoForPVToAggregate(aggregateInfo, pvName, typeInfo);
        // Add ETL storage impact computation
        synchronized (aggregateInfo) {
            if (typeInfo.getDataStores() != null && typeInfo.getDataStores().length > 0) {
                for (String dataStore : typeInfo.getDataStores()) {
                    try {
                        org.epics.archiverappliance.etl.ETLDest etlDest =
                                StoragePluginURLParser.parseETLDest(dataStore, this);
                        if (etlDest instanceof org.epics.archiverappliance.etl.StorageMetrics) {
                            org.epics.archiverappliance.etl.StorageMetrics stMetrics =
                                    (org.epics.archiverappliance.etl.StorageMetrics) etlDest;
                            String identity = stMetrics.getName();
                            double storageImpact = etlDest.getPartitionGranularity().getApproxSecondsPerChunk()
                                    * typeInfo.getComputedStorageRate();
                            java.util.HashMap<String, Long> totalStorageImpact = aggregateInfo.getTotalStorageImpact();
                            if (!totalStorageImpact.containsKey(identity)) {
                                totalStorageImpact.put(identity, Long.valueOf(0));
                            }
                            long currentStorageImpact = totalStorageImpact.get(identity);
                            currentStorageImpact += storageImpact;
                            totalStorageImpact.put(identity, currentStorageImpact);
                        }
                    } catch (Exception ex) {
                        logger.error("Exception parsing storage metrics url " + dataStore, ex);
                    }
                }
            }
        }
    }

    @Override
    protected void loadExternalArchiverPVs(String serverURL, String archive) throws IOException, SAXException {
        if (archive.equals("pbraw")) {
            logger.debug(
                    "We do not load PV names from external EPICS archiver appliances. These can number in the multiple millions and the respone on retrieval is fast enough anyways");
            return;
        }

        ChannelArchiverDataServerInfo serverInfo = new ChannelArchiverDataServerInfo(serverURL, archive);
        NamesHandler handler = new NamesHandler();
        logger.debug(
                () -> "Getting list of PV's from Channel Archiver Server at " + serverURL + " using index " + archive);
        XMLRPCClient.archiverNames(serverURL, archive, handler);
        HashMap<String, List<ChannelArchiverDataServerPVInfo>> tempPVNames =
                new HashMap<String, List<ChannelArchiverDataServerPVInfo>>();
        long totalPVsProxied = 0;
        for (NamesHandler.ChannelDescription pvChannelDesc : handler.getChannels()) {
            String pvName = PVNames.normalizeChannelName(pvChannelDesc.getName());
            if (this.pv2ChannelArchiverDataServer.containsKey(pvName)) {
                List<ChannelArchiverDataServerPVInfo> alreadyExistingServers =
                        this.pv2ChannelArchiverDataServer.get(pvName);
                logger.debug(() -> "Adding new server to already existing ChannelArchiver server for " + pvName);
                addExternalCAServerToExistingList(alreadyExistingServers, serverInfo, pvChannelDesc);
                tempPVNames.put(pvName, alreadyExistingServers);
            } else if (tempPVNames.containsKey(pvName)) {
                List<ChannelArchiverDataServerPVInfo> alreadyExistingServers = tempPVNames.get(pvName);
                logger.debug(
                        "Adding new server to already existing ChannelArchiver server (in tempspace) for " + pvName);
                addExternalCAServerToExistingList(alreadyExistingServers, serverInfo, pvChannelDesc);
                tempPVNames.put(pvName, alreadyExistingServers);
            } else {
                List<ChannelArchiverDataServerPVInfo> caServersForPV = new ArrayList<ChannelArchiverDataServerPVInfo>();
                caServersForPV.add(new ChannelArchiverDataServerPVInfo(
                        serverInfo, pvChannelDesc.getStartSec(), pvChannelDesc.getEndSec()));
                tempPVNames.put(pvName, caServersForPV);
            }

            if (tempPVNames.size() > 1000) {
                this.pv2ChannelArchiverDataServer.putAll(tempPVNames);
                totalPVsProxied += tempPVNames.size();
                tempPVNames.clear();
            }
        }
        if (!tempPVNames.isEmpty()) {
            this.pv2ChannelArchiverDataServer.putAll(tempPVNames);
            totalPVsProxied += tempPVNames.size();
            tempPVNames.clear();
        }
        if (logger.isDebugEnabled())
            logger.debug("Proxied a total of " + totalPVsProxied + " from server " + serverURL + " using archive "
                    + archive);
    }

    private static void addExternalCAServerToExistingList(
            List<ChannelArchiverDataServerPVInfo> alreadyExistingServers,
            ChannelArchiverDataServerInfo serverInfo,
            NamesHandler.ChannelDescription pvChannelDesc) {
        List<ChannelArchiverDataServerPVInfo> copyOfAlreadyExistingServers = new LinkedList<>();
        for (ChannelArchiverDataServerPVInfo alreadyExistingServer : alreadyExistingServers) {
            if (alreadyExistingServer.getServerInfo().equals(serverInfo)) {
                logger.debug(() -> "Removing a channel archiver server that already exists " + alreadyExistingServer);
            } else {
                copyOfAlreadyExistingServers.add(alreadyExistingServer);
            }
        }

        int beforeCount = alreadyExistingServers.size();
        alreadyExistingServers.clear();
        alreadyExistingServers.addAll(copyOfAlreadyExistingServers);

        // Readd the CA server - this should take into account any updated start times, end times and so on.
        alreadyExistingServers.add(new ChannelArchiverDataServerPVInfo(
                serverInfo, pvChannelDesc.getStartSec(), pvChannelDesc.getEndSec()));

        int afterCount = alreadyExistingServers.size();
        logger.debug(() -> "We had " + beforeCount + " and now we have " + afterCount
                + " when adding external ChannelArchiver server");

        // Sort the servers by ascending time stamps before adding it back.
        ChannelArchiverDataServerPVInfo.sortServersBasedOnStartAndEndSecs(alreadyExistingServers);
    }

    // =========================================================================
    // Abstract method implementations (require ExecutePolicy)
    // =========================================================================

    @Override
    public PolicyConfig computePolicyForPV(String pvName, MetaInfo metaInfo, UserSpecifiedSamplingParams userSpecParams)
            throws IOException {
        try (java.io.InputStream is = this.getPolicyText()) {
            logger.debug(() -> "Computing policy for pvName");
            HashMap<String, Object> pvInfo = new HashMap<String, Object>();
            pvInfo.put("dbrtype", metaInfo.getArchDBRTypes().toString());
            pvInfo.put("elementCount", metaInfo.getCount());
            pvInfo.put("eventRate", metaInfo.getEventRate());
            pvInfo.put("eventCount", metaInfo.getEventCount());
            pvInfo.put("storageRate", metaInfo.getStorageRate());
            pvInfo.put("aliasName", metaInfo.getAliasName());
            if (userSpecParams != null && userSpecParams.getPolicyName() != null) {
                logger.debug(() -> "Passing user override of policy " + userSpecParams.getPolicyName()
                        + " as the dict entry policyName");
                pvInfo.put("policyName", userSpecParams.getPolicyName());
            }
            if (userSpecParams.getControllingPV() != null) {
                pvInfo.put("controlPV", userSpecParams.getControllingPV());
            }

            HashMap<String, String> otherMetaInfo = metaInfo.getOtherMetaInfo();
            for (String otherMetaInfoKey : this.getExtraFields()) {
                if (otherMetaInfo.containsKey(otherMetaInfoKey)) {
                    if (otherMetaInfoKey.equals("ADEL") || otherMetaInfoKey.equals("MDEL")) {
                        try {
                            pvInfo.put(otherMetaInfoKey, Double.parseDouble(otherMetaInfo.get(otherMetaInfoKey)));
                        } catch (Exception ex) {
                            logger.error("Exception adding MDEL and ADEL to the info", ex);
                        }
                    } else {
                        pvInfo.put(otherMetaInfoKey, otherMetaInfo.get(otherMetaInfoKey));
                    }
                }
            }

            if (logger.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder();
                buf.append("Before computing policy for");
                buf.append(pvName);
                buf.append(" pvInfo is \n");
                for (String key : pvInfo.keySet()) {
                    buf.append(key);
                    buf.append("=");
                    buf.append(pvInfo.get(key));
                    buf.append("\n");
                }
                logger.debug(buf.toString());
            }

            try {
                // We only have one policy in the cache...
                ExecutePolicy executePolicy = theExecutionPolicy.get("ThePolicy");
                return executePolicy.computePolicyForPV(pvName, pvInfo);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                logger.error("Exception executing policy for pv " + pvName, cause);
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                } else {
                    throw new IOException(cause);
                }
            }
        }
    }

    @Override
    public HashMap<String, String> getPoliciesInInstallation() throws IOException {
        try (ExecutePolicy executePolicy = new ExecutePolicy(this)) {
            return executePolicy.getPolicyList();
        }
    }

    @Override
    public List<String> getFieldsArchivedAsPartOfStream() throws IOException {
        try {
            ExecutePolicy executePolicy = theExecutionPolicy.get("ThePolicy");
            return executePolicy.getFieldsArchivedAsPartOfStream();
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }

    // =========================================================================
    // Service-specific runtime state getters
    // =========================================================================

    @Override
    public EngineContext getEngineContext() {
        return engineContext;
    }

    @Override
    public RetrievalState getRetrievalRuntimeState() {
        return retrievalState;
    }

    @Override
    public PBThreeTierETLPVLookup getETLLookup() {
        return etlPVLookup;
    }

    @Override
    public MgmtRuntimeState getMgmtRuntimeState() {
        return mgmtRuntime;
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /**
     * Get the PVs that belong to this appliance and start archiving them.
     * Needless to say, this gets done only in the engine.
     */
    private void archivePVSonStartup() {
        configlogger.debug(() -> "Start archiving PVs from persistence.");
        // To prevent broadcast storms, we pause for pausePerGroup seconds for every pausePerGroup PVs
        int currentPVCount = 0;
        int pausePerGroupPVCount = Integer.parseInt(this.getInstallationProperties()
                .getProperty("org.epics.archiverappliance.engine.archivePVSonStartup.pausePerGroupPVCount", "2000"));
        int pausePerGroupPauseTimeInSeconds = Integer.parseInt(this.getInstallationProperties()
                .getProperty(
                        "org.epics.archiverappliance.engine.archivePVSonStartup.pausePerGroupPauseTimeInSeconds", "2"));
        boolean determineLastKnownEventFromStores = Boolean.parseBoolean(this.getInstallationProperties()
                .getProperty(
                        "org.epics.archiverappliance.engine.archivePVSonStartup.determineLastKnownEventFromStores",
                        "true"));

        for (String pvName : this.getPVsForThisAppliance()) {
            try {
                PVTypeInfo typeInfo = typeInfos.get(pvName);
                if (typeInfo == null) {
                    logger.error("On restart, cannot find typeinfo for pv " + pvName + ". Not archiving");
                    continue;
                }

                if (typeInfo.isPaused()) {
                    logger.debug(() -> "Skipping archiving paused PV " + pvName + " on startup");
                    continue;
                }

                org.epics.archiverappliance.data.ArchDBRTypes dbrType = typeInfo.getDBRType();
                float samplingPeriod = typeInfo.getSamplingPeriod();
                SamplingMethod samplingMethod = typeInfo.getSamplingMethod();
                StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(typeInfo.getDataStores()[0], this);

                Instant lastKnownTimestamp = null;
                if (determineLastKnownEventFromStores) {
                    lastKnownTimestamp = typeInfo.determineLastKnownEventFromStores(this);
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Last known timestamp from ETL stores is for pv {} is {} ",
                                pvName,
                                TimeUtils.convertToHumanReadableString(lastKnownTimestamp));
                    }
                }

                ArchiveEngine.archivePV(
                        pvName,
                        samplingPeriod,
                        samplingMethod,
                        firstDest,
                        this,
                        dbrType,
                        lastKnownTimestamp,
                        typeInfo.getControllingPV(),
                        typeInfo.getArchiveFields(),
                        typeInfo.getHostName(),
                        typeInfo.isUsePVAccess(),
                        typeInfo.isUseDBEProperties());
                currentPVCount++;
                if (currentPVCount % pausePerGroupPVCount == 0) {
                    logger.debug(
                            () -> "Sleeping for " + pausePerGroupPauseTimeInSeconds + " to prevent CA search storms");
                    Thread.sleep(pausePerGroupPauseTimeInSeconds * 1000);
                }
            } catch (Throwable t) {
                logger.error("Exception starting up archiving of PV " + pvName + ". Moving on to the next pv.", t);
            }
        }
        configlogger.debug("Started " + currentPVCount + " PVs from persistence.");
    }
}
