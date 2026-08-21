package org.epics.archiverappliance.engine.pv;

import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PolicyService;
import org.epics.archiverappliance.config.StoragePluginConfigView;

public class PVFactory {
    /**
     * This is the constructor used by the MetaGet's - this is the initial step in adding a PV to the archiver where we determine some facts about the PV for the policies
     * @param name  The PV name.
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param jcaCommandThreadId The JCA Command thread.
     * @param usePVAccess  &emsp;
     * @return PV  &emsp;
     */
    public static PV createPV(
            String name,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService,
            int jcaCommandThreadId,
            boolean usePVAccess) {
        if (usePVAccess) {
            return new EPICS_V4_PV(name, storageConfig, applianceLifecycle, jcaCommandThreadId);
        } else {
            return new EPICS_V3_PV(name, storageConfig, applianceLifecycle, policyService, jcaCommandThreadId);
        }
    }

    /**
     * This is the constructor used by the ArchiveChannel to create the main PV.
     * @param name The PV name.
     * @param storageConfig The configuration the storage plugins and type system come from
     * @param applianceLifecycle Keys this appliance's engine context
     * @param policyService The policy fields archived with the stream
     * @param isControlPV  &emsp;
     * @param archDBRTypes ArchDBRTypes
     * @param jcaCommandThreadId  The JCA Command thread.
     * @param usePVAccess Should we use PVAccess to connect to this PV.
     * @param useDBEProperties &emsp;
     * @return PV &emsp;
     */
    public static PV createPV(
            final String name,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService,
            boolean isControlPV,
            ArchDBRTypes archDBRTypes,
            int jcaCommandThreadId,
            boolean usePVAccess,
            boolean useDBEProperties) {
        if (usePVAccess) {
            return new EPICS_V4_PV(
                    name, storageConfig, applianceLifecycle, isControlPV, archDBRTypes, jcaCommandThreadId);
        } else {
            return new EPICS_V3_PV(
                    name,
                    storageConfig,
                    applianceLifecycle,
                    policyService,
                    isControlPV,
                    archDBRTypes,
                    jcaCommandThreadId,
                    useDBEProperties);
        }
    }

    public static ControllingPV createControllingPV(
            final String name,
            StoragePluginConfigView storageConfig,
            ApplianceLifecycle applianceLifecycle,
            PolicyService policyService,
            boolean isControlPV,
            ArchDBRTypes archDBRTypes,
            int jcaCommandThreadId,
            boolean usePVAccess) {
        //		if(usePVAccess) {
        //			// TODO Make EPICS_V4_PV implement controlling PV.
        //			// return new EPICS_V4_PV(name, storageConfig, applianceLifecycle, isControlPV, archDBRTypes,
        // jcaCommandThreadId);
        //		} else {
        return new EPICS_V3_PV(
                name,
                storageConfig,
                applianceLifecycle,
                policyService,
                isControlPV,
                archDBRTypes,
                jcaCommandThreadId,
                false);
        //		}
    }
}
