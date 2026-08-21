/*******************************************************************************
 *
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 *
 * as Operator of the SLAC National Accelerator Laboratory.
 *
 * Copyright (c) 2011 Brookhaven National Laboratory.
 *
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 *
 * in file LICENSE that is included with this distribution.
 *
 *******************************************************************************/
package org.epics.archiverappliance.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.PolicyService;
import org.epics.archiverappliance.config.StoragePluginConfigView;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.epics.archiverappliance.engine.metadata.MetaGet;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.DeltaArchiveChannel;
import org.epics.archiverappliance.engine.model.MonitoredArchiveChannel;
import org.epics.archiverappliance.engine.model.SampleMode;
import org.epics.archiverappliance.engine.model.ScannedArchiveChannel;
import org.epics.archiverappliance.engine.pv.ControllingPV;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVFactory;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class provides the static methods:
 * <ol>
 * <li>create channel</li>
 * <li>get pv meta data before archiving this pv</li>
 * <li>pause archiving pv</li>
 * <li>resume archiving pv</li>
 * <li>get metics of one pv</li>
 * <li>change the archival parameter of one pv </li>
 * <li>destory pv</li>
 * </ol>
 * @author Luofeng Li
 *
 */
public class ArchiveEngine {
    private static final Logger logger = LogManager.getLogger(ArchiveEngine.class.getName());

    /**
     * Create the channel for the PV and register it in the places where it needs to be registered.
     * @param name  &emsp;
     * @param writer  First destination
     * @param enablement Enablement
     * @param sample_mode SampleMode
     * @param last_sampleTimestamp Instant
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param archdbrtype   ArchDBRTypes
     * @param controlPVname  &emsp;
     * @param iocHostName - Can be null.
     * @return Archivechanel &emsp;
     * @throws Exception &emsp;
     */
    private static ArchiveChannel addChannel(
            final String name,
            final Writer writer,
            final SampleMode sample_mode,
            final Instant last_sampleTimestamp,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final ArchDBRTypes archdbrtype,
            final String controlPVname,
            final String iocHostName,
            final boolean usePVAccess)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = null;
        // Is this an existing channel?
        channel = engineContext.getChannelList().get(name);
        if (channel != null) {
            logger.debug(String.format(" Channel '%s' already exist'", name));
            return channel;
        }

        // Determine buffer capacity
        double write_period = EngineContext.of(applianceLifecycle).getWritePeriod();
        double pvSamplingPeriod = sample_mode.getPeriod();
        if (pvSamplingPeriod <= 0.0) {
            logger.warn("Sampling period is invalid " + pvSamplingPeriod + ". Resetting this to "
                    + PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD);
            pvSamplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
        }

        int buffer_capacity = ((int) Math.round(Math.max(
                        (write_period / pvSamplingPeriod) * engineContext.getSampleBufferCapacityAdjustment(), 1.0)))
                + 1;
        if (buffer_capacity < 2) {
            logger.debug("Enforcing a minimum capacity for sample buffer size.");
            buffer_capacity = 2;
        }
        logger.debug("Final buffer capacity for pv " + name + " is " + buffer_capacity);

        int JCACommandThreadID = engineContext.assignJCACommandThread(name, iocHostName);

        // Create new channel
        if (sample_mode.isMonitor()) {
            if (sample_mode.getDelta() > 0) {
                channel = new DeltaArchiveChannel(
                        name,
                        writer,
                        buffer_capacity,
                        last_sampleTimestamp,
                        pvSamplingPeriod,
                        sample_mode.getDelta(),
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        archdbrtype,
                        controlPVname,
                        JCACommandThreadID,
                        usePVAccess);
            } else {
                channel = new MonitoredArchiveChannel(
                        name,
                        writer,
                        buffer_capacity,
                        last_sampleTimestamp,
                        pvSamplingPeriod,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        archdbrtype,
                        controlPVname,
                        JCACommandThreadID,
                        usePVAccess);
            }
        } else {
            channel = new ScannedArchiveChannel(
                    name,
                    writer,
                    buffer_capacity,
                    last_sampleTimestamp,
                    pvSamplingPeriod,
                    storageConfig,
                    applianceLifecycle,
                    policyService,
                    archdbrtype,
                    controlPVname,
                    JCACommandThreadID,
                    usePVAccess);
        }

        EngineContext.of(applianceLifecycle).getChannelList().put(channel.getName(), channel);
        engineContext.getWriteThead().addChannel(channel);
        return channel;
    }

    private static void createChannels4PVWithMetaField(
            final String pvName,
            final float samplingPeriod,
            final SamplingMethod mode,
            final Writer writer,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final ArchDBRTypes archdbrtype,
            final Instant lastKnownEventTimeStamp,
            final boolean start,
            final String controlPVname,
            final String[] metaFields,
            final String iocHostName,
            final boolean usePVAccess,
            final boolean useDBEProperties)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);

        if (!engineContext.isWriteThreadStarted()) {
            engineContext.startWriteThread(storageConfig);
        }

        if (mode == SamplingMethod.SCAN) {
            SampleMode scan_mode2 = new SampleMode(false, 0, samplingPeriod);
            ArchiveChannel channel = ArchiveEngine.addChannel(
                    pvName,
                    writer,
                    scan_mode2,
                    lastKnownEventTimeStamp,
                    storageConfig,
                    applianceLifecycle,
                    policyService,
                    archdbrtype,
                    controlPVname,
                    iocHostName,
                    usePVAccess);

            if (start) {
                channel.start();
            }

            engineContext
                    .getScanScheduler()
                    .scheduleAtFixedRate(
                            (ScannedArchiveChannel) channel, 0, (long) (samplingPeriod * 1000), TimeUnit.MILLISECONDS);

            channel.initializeMetaFieldPVS(
                    metaFields, storageConfig, applianceLifecycle, policyService, usePVAccess, useDBEProperties);
        } else if (mode == SamplingMethod.MONITOR) {
            SampleMode scan_mode2 = new SampleMode(true, 0, samplingPeriod);
            ArchiveChannel channel = ArchiveEngine.addChannel(
                    pvName,
                    writer,
                    scan_mode2,
                    lastKnownEventTimeStamp,
                    storageConfig,
                    applianceLifecycle,
                    policyService,
                    archdbrtype,
                    controlPVname,
                    iocHostName,
                    usePVAccess);

            if (start) {
                channel.start();
            }

            // handle the meta field
            channel.initializeMetaFieldPVS(
                    metaFields, storageConfig, applianceLifecycle, policyService, usePVAccess, useDBEProperties);
        } else if (mode == SamplingMethod.DONT_ARCHIVE) {
            // Do nothing..
        }
    }

    /**
     * Get the meta data for pv - used for policy computation.
     *
     * @param pvName Name of the channel (PV)
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param metadatafields other field such as MDEL,ADEL, except basical info in DBR_CTRL
     * @param usePVAccess  Should we use PV access to connect to this PV.
     * @param metaListener the callback interface where you handle the info.
     * @throws Exception On error in getting pv's info
     */
    public static void getArchiveInfo(
            final String pvName,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final String metadatafields[],
            boolean usePVAccess,
            final MetaCompletedListener metaListener)
            throws Exception {
        MetaGet metaget = new MetaGet(
                pvName, storageConfig, applianceLifecycle, policyService, metadatafields, usePVAccess, metaListener);
        metaget.initpv();
    }

    /**
     * @param pvName Name of the channel (PV)
     * @param samplingPeriod The minimal sample period for channel in scan mode.  Attention: the same data with same value and timestamp is not saved again in scan mode. This period is meanlingless for channel in monitor mode.
     * @param mode scan or monitor
     * @param writer First destination
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param archdbrtype Expected DBR type.
     * @param lastKnownEventTimeStamp Last known event from all the stores.
     * @param controllingPVName The PV that controls archiving for this PV
     * @param usePVAccess Should we use PVAccess to connect to this PV.
     * @param useDBEProperties  &emsp;
     * @throws Exception &emsp;
     */
    public static void archivePV(
            final String pvName,
            final float samplingPeriod,
            final SamplingMethod mode,
            final Writer writer,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final ArchDBRTypes archdbrtype,
            final Instant lastKnownEventTimeStamp,
            final String controllingPVName,
            final boolean usePVAccess,
            final boolean useDBEProperties)
            throws Exception {
        archivePV(
                pvName,
                samplingPeriod,
                mode,
                writer,
                storageConfig,
                applianceLifecycle,
                policyService,
                archdbrtype,
                lastKnownEventTimeStamp,
                controllingPVName,
                null,
                null,
                usePVAccess,
                useDBEProperties);
    }

    /**
     * @param pvName Name of the channel (PV)
     * @param samplingPeriod The minimal sample period for channel in scan mode.  Attention: the same data with same value and timestamp is not saved again in scan mode. This period is meanlingless for channel in monitor mode.
     * @param mode scan or monitor
     * @param writer First destination
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param archdbrtype Expected DBR type.
     * @param lastKnownEventTimeStamp Last known event from all the stores.
     * @param usePVAccess Should we use PVAccess to connect to this PV.
     * @param useDBEProperties  &emsp;
     * @throws Exception &emsp;
     */
    public static void archivePV(
            final String pvName,
            final float samplingPeriod,
            final SamplingMethod mode,
            final Writer writer,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final ArchDBRTypes archdbrtype,
            final Instant lastKnownEventTimeStamp,
            final boolean usePVAccess,
            final boolean useDBEProperties)
            throws Exception {
        archivePV(
                pvName,
                samplingPeriod,
                mode,
                writer,
                storageConfig,
                applianceLifecycle,
                policyService,
                archdbrtype,
                lastKnownEventTimeStamp,
                null,
                null,
                null,
                usePVAccess,
                useDBEProperties);
    }

    /**
     * @param pvName Name of the channel (PV)
     * @param samplingPeriod The minimal sample period for channel in scan mode.  Attention: the same data with same value and timestamp is not saved again in scan mode. This period is meanlingless for channel in monitor mode.
     * @param mode scan or monitor
     * @param writer First destination
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param archdbrtype Expected DBR type.
     * @param lastKnownEventTimeStamp Last known event from all the stores.
     * @param metaFieldNames An array of EPICS fields that gets stored along with the stream. Needs rethinking once we have EPICS V4
     * @param usePVAccess Should we use PVAccess to connect to this PV.
     * @param useDBEProperties  &emsp;
     * @throws Exception &emsp;
     */
    public static void archivePV(
            final String pvName,
            final float samplingPeriod,
            final SamplingMethod mode,
            final Writer writer,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final ArchDBRTypes archdbrtype,
            final Instant lastKnownEventTimeStamp,
            final String[] metaFieldNames,
            final boolean usePVAccess,
            final boolean useDBEProperties)
            throws Exception {
        archivePV(
                pvName,
                samplingPeriod,
                mode,
                writer,
                storageConfig,
                applianceLifecycle,
                policyService,
                archdbrtype,
                lastKnownEventTimeStamp,
                null,
                metaFieldNames,
                null,
                usePVAccess,
                useDBEProperties);
    }

    /**
     * Create a new channel in monitor mode or in scan mode
     * @param pvName Name of the channel (PV)
     * @param samplingPeriod The minimal sample period for channel in scan mode.  Attention: the same data with same value and timestamp is not saved again in scan mode. This period is meanlingless for channel in monitor mode.
     * @param mode scan or monitor
     * @param writer First destination
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param archdbrtype Expected DBR type.
     * @param lastKnownEventTimeStamp Last known event from all the stores.
     * @param controllingPVName The PV that controls archiving for this PV
     * @param metaFieldNames An array of EPICS fields that gets stored along with the stream. Needs rethinking once we have EPICS V4
     * @param iocHostName IOC hosting this PV; this is used for some optimization. This will often be null.
     * @param usePVAccess Should we use PVAccess to connect to this PV.
     * @param useDBEProperties  &emsp;
     * @throws Exception &emsp;
     */
    public static void archivePV(
            final String pvName,
            final float samplingPeriod,
            final SamplingMethod mode,
            final Writer writer,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final ArchDBRTypes archdbrtype,
            final Instant lastKnownEventTimeStamp,
            final String controllingPVName,
            final String[] metaFieldNames,
            final String iocHostName,
            final boolean usePVAccess,
            final boolean useDBEProperties)
            throws Exception {

        boolean start = true;
        if (controllingPVName != null) {
            ConcurrentHashMap<String, ControllingPV> controlingPVList =
                    EngineContext.of(applianceLifecycle).getControlingPVList();
            ControllingPV controllingPV = controlingPVList.get(controllingPVName);

            if (controllingPV == null) {
                ArchiveEngine.createChannels4PVWithMetaField(
                        pvName,
                        samplingPeriod,
                        mode,
                        writer,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        archdbrtype,
                        lastKnownEventTimeStamp,
                        start,
                        controllingPVName,
                        metaFieldNames,
                        iocHostName,
                        usePVAccess,
                        useDBEProperties);
                controllingPV = PVFactory.createControllingPV(
                        controllingPVName,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        true,
                        archdbrtype,
                        EngineContext.of(applianceLifecycle).assignJCACommandThread(controllingPVName, null),
                        false);
                controlingPVList.put(controllingPVName, controllingPV);
                controllingPV.addControledPV(pvName);
                controllingPV.start();
            } else {
                controllingPV.addControledPV(pvName);
                start = controllingPV.isEnableAllPV();

                ArchiveEngine.createChannels4PVWithMetaField(
                        pvName,
                        samplingPeriod,
                        mode,
                        writer,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        archdbrtype,
                        lastKnownEventTimeStamp,
                        start,
                        controllingPVName,
                        metaFieldNames,
                        iocHostName,
                        usePVAccess,
                        useDBEProperties);
            }
        } else {
            ArchiveEngine.createChannels4PVWithMetaField(
                    pvName,
                    samplingPeriod,
                    mode,
                    writer,
                    storageConfig,
                    applianceLifecycle,
                    policyService,
                    archdbrtype,
                    lastKnownEventTimeStamp,
                    start,
                    null,
                    metaFieldNames,
                    iocHostName,
                    usePVAccess,
                    useDBEProperties);
        }
    }

    /**
     * pause the pv
     *
     * @param pvName Name of the channel (PV)
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @throws Exception error in pausing the channel .
     */
    public static void pauseArchivingPV(
            final String pvName,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        // pause the pv
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);
        if (channel != null) {
            channel.shutdownMetaChannels();
            channel.stop();
            channel.setPaused(true);
            destoryPv(pvName, applianceLifecycle);
        }
    }

    /**
     * restart the pv
     *
     * @param pvName
     *            Name of the channel (PV)
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @throws Exception
     *              error in restarting the channel .
     */
    public static void resumeArchivingPV(
            final String pvName,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);
        if (channel != null) {
            if (channel.isRunning() && !channel.isPaused()) {
                // Restarting a live channel drops and recreates the CA/PVA monitor, losing events and
                // recording a spurious connection loss. Archival parameter changes are applied via the
                // explicit engine BPLs, so resuming an already-archiving PV is a no-op.
                logger.debug("PV " + pvName + " is already being archived; not restarting the channel on resume");
                return;
            }
            channel.stop();
            channel.start();
            channel.setPaused(false);
        } else {
            // We have not created the channel on startup.
            // We should start it up
            logger.debug("We had not created the channel on startup. Creating it " + pvName);
            startChannelsForPV(pvName, storageConfig, applianceLifecycle, policyService);
        }
    }

    /**
     * restart the pv
     *
     * @param pvName        Name of the channel (PV)
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @throws Exception error in restarting the channel .
     */
    public static void resumeArchivingPV(
            final String pvName,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService,
            Writer writer)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);
        if (channel != null) {
            if (channel.isRunning() && !channel.isPaused()) {
                logger.debug("PV " + pvName + " is already being archived; not restarting the channel on resume");
                return;
            }
            channel.stop();
            channel.start();
        } else {
            // We have not created the channel on startup.
            // We should start it up
            logger.debug("We had not created the channel on startup. Creating it " + pvName);
            startChannelsForPV(
                    pvName,
                    storageConfig,
                    applianceLifecycle,
                    policyService,
                    storageConfig.getTypeInfoForPV(pvName),
                    writer);
        }
    }

    /**
     * Start up the channels for a PV.
     * Should be called on startup or on resume of a PV that was paused on startup.
     * @param pvName The Name of PV.
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @throws IOException  &emsp;
     * @throws Exception  &emsp;
     */
    public static void startChannelsForPV(
            final String pvName,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService)
            throws IOException, Exception {
        logger.debug("Starting up channels for pv " + pvName);
        PVTypeInfo typeInfo = storageConfig.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.error("Cannot resume PV for which we cannot typeinfo " + pvName);
            return;
        }
        StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(typeInfo.getDataStores()[0], storageConfig);
        startChannelsForPV(pvName, storageConfig, applianceLifecycle, policyService, typeInfo, firstDest);
    }

    /**
     * Start up the channels for a PV.
     * Should be called on startup or on resume of a PV that was paused on startup.
     * @param pvName The Name of PV.
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @throws IOException  &emsp;
     * @throws Exception  &emsp;
     */
    public static void startChannelsForPV(
            final String pvName,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService,
            PVTypeInfo typeInfo,
            Writer writer)
            throws IOException, Exception {
        logger.debug("Starting up channels for pv " + pvName);
        if (typeInfo == null) {
            logger.error("Cannot resume PV for which we cannot typeinfo " + pvName);
            return;
        }
        ArchDBRTypes dbrType = typeInfo.getDBRType();
        float samplingPeriod = typeInfo.getSamplingPeriod();
        SamplingMethod samplingMethod = typeInfo.getSamplingMethod();
        Instant lastKnownTimestamp = typeInfo.determineLastKnownEventFromStores(storageConfig);
        if (logger.isDebugEnabled())
            logger.debug("Last known timestamp from ETL stores is for pv " + pvName + " is "
                    + TimeUtils.convertToHumanReadableString(lastKnownTimestamp));

        ArchiveEngine.archivePV(
                pvName,
                samplingPeriod,
                samplingMethod,
                writer,
                storageConfig,
                applianceLifecycle,
                policyService,
                dbrType,
                lastKnownTimestamp,
                typeInfo.getControllingPV(),
                typeInfo.getArchiveFields(),
                typeInfo.getHostName(),
                typeInfo.isUsePVAccess(),
                typeInfo.isUseDBEProperties());
    }

    /**
     * get the pv's info and status
     * @param pvName
     *            Name of the channel (PV)
     * @param applianceLifecycle  ConfigService
     * @return  PVMetrics  &emsp;
     * @throws Exception
     *             On error in getting the pv info and status .
     */
    public static PVMetrics getMetricsforPV(String pvName, ApplianceLifecycle applianceLifecycle) throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);
        if (channel == null) {
            return null;
        }
        return channel.getPVMetrics();
    }

    /**
     * Return info from CAJ
     * @param pvName The name of PV.
     * @param applianceLifecycle  ConfigService
     * @param statuses  Add a list of key value pairs to the status
     * @throws Exception  &emsp;
     */
    public static void getLowLevelStateInfo(
            String pvName, ApplianceLifecycle applianceLifecycle, LinkedList<Map<String, String>> statuses)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);
        if (channel == null) return;
        channel.getLowLevelChannelStateInfo(statuses);
    }

    /**
     * change pv's sample period or sample mode
     *
     * @param pvName
     *            Name of the channel (PV)
     * @param samplingPeriod
     *            new sampling Period of the channel (PV)
     * @param mode scan or monitor
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param writer
     *            the writer to protocol buffer
     * @param usePVAccess Should we use PVAccess to connect to this PV.
     * @param useDBEPropeties  &emsp;
     * @throws Exception
     *             On error in getting the pv info and status .
     */
    public static void changeArchivalParameters(
            final String pvName,
            final float samplingPeriod,
            final SamplingMethod mode,
            final StoragePluginConfigView storageConfig,
            final ApplianceLifecycle applianceLifecycle,
            final PolicyService policyService,
            final Writer writer,
            final boolean usePVAccess,
            final boolean useDBEPropeties)
            throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);
        if (channel == null) {
            throw new Exception(String.format(" Channel '%s' doesn't exist'", pvName));
        }

        PVMetrics pvMetrics = channel.getPVMetrics();
        boolean isMonitor = pvMetrics.isMonitor();
        double samplePeriodOld = pvMetrics.getSamplingPeriod();
        if (mode == SamplingMethod.SCAN) {
            if (isMonitor) {
                // mode is changed from monitor to scan,new mode is scan
                // stop channel and remove id from ChannelList and buffer
                channel.stop();
                engineContext.getWriteThead().removeChannel(pvName);
                engineContext.getChannelList().remove(pvName);
                // add new channel in scan mode
                ArchiveEngine.archivePV(
                        pvName,
                        samplingPeriod,
                        SamplingMethod.SCAN,
                        writer,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        pvMetrics.getArchDBRTypes(),
                        null,
                        usePVAccess,
                        useDBEPropeties);
            } else {
                // mode is not changed the mode is still scan
                // the new sample period is the same with the old sample period
                double perioddelt = Math.abs(samplePeriodOld - samplingPeriod);
                if (perioddelt < 0.1) {
                    // the same sample period
                    // do nothing
                } else {
                    // different period
                    engineContext.getScanScheduler().remove((ScannedArchiveChannel) channel);
                    engineContext.getScanScheduler().purge();
                    // stop channel and remove id from ChannelList and buffer

                    channel.stop();
                    engineContext.getWriteThead().removeChannel(pvName);
                    engineContext.getChannelList().remove(pvName);
                    // add new channel in scan mode
                    ArchiveEngine.archivePV(
                            pvName,
                            samplingPeriod,
                            SamplingMethod.SCAN,
                            writer,
                            storageConfig,
                            applianceLifecycle,
                            policyService,
                            pvMetrics.getArchDBRTypes(),
                            null,
                            usePVAccess,
                            useDBEPropeties);
                }
            }
        } else if (mode == SamplingMethod.MONITOR) {
            if (isMonitor) {
                // mode is not changed, is monior
                // remove the channel in monitor mode
                channel.stop();
                engineContext.getWriteThead().removeChannel(pvName);
                engineContext.getChannelList().remove(pvName);
                // add new channel in monitor mode
                ArchiveEngine.archivePV(
                        pvName,
                        samplingPeriod,
                        SamplingMethod.MONITOR,
                        writer,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        pvMetrics.getArchDBRTypes(),
                        null,
                        usePVAccess,
                        useDBEPropeties);
            } else {
                // mode is changed from scan to monitor ,new mode is monitor
                engineContext.getScanScheduler().remove((ScannedArchiveChannel) channel);
                engineContext.getScanScheduler().purge();
                channel.stop();
                engineContext.getWriteThead().removeChannel(pvName);
                engineContext.getChannelList().remove(pvName);

                // add new channel in monitor mode
                ArchiveEngine.archivePV(
                        pvName,
                        samplingPeriod,
                        SamplingMethod.MONITOR,
                        writer,
                        storageConfig,
                        applianceLifecycle,
                        policyService,
                        pvMetrics.getArchDBRTypes(),
                        null,
                        usePVAccess,
                        useDBEPropeties);
            }
        }
    }

    /**
     * destroy the pv
     * @param pvName  the pv's name to destroy
     * @param applianceLifecycle the configSerivice of the application
     * @throws Exception
     *        error when destroy the PV
     */
    public static void destoryPv(String pvName, final ApplianceLifecycle applianceLifecycle) throws Exception {
        EngineContext engineContext = EngineContext.of(applianceLifecycle);
        ArchiveChannel channel = engineContext.getChannelList().get(pvName);

        if (channel == null) {
            logger.debug("Skipping deleting PV that does not have channel info " + pvName);
            return;
        }

        PVMetrics pvMetrics = channel.getPVMetrics();
        boolean isMonitor = pvMetrics.isMonitor();
        if (isMonitor) {
            // pv is in monitor mode
            // remove the channel in monitor mode
        } else {
            // pv is in scan mode
            // remove the channel in scan mode
            engineContext.getScanScheduler().remove((ScannedArchiveChannel) channel);
            engineContext.getScanScheduler().purge();
        }
        channel.stop();
        engineContext.getWriteThead().removeChannel(pvName);
        engineContext.getChannelList().remove(pvName);
    }
}
