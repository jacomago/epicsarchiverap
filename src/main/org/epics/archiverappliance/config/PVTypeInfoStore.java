package org.epics.archiverappliance.config;

import java.io.Serializable;
import java.util.Collection;

/**
 * Interface for storing and retrieving PV type information.
 */
public interface PVTypeInfoStore {
    /**
     * Extracts the parts of a PVTypeInfo that a caller of {@link #queryPVTypeInfos} is interested in.
     * The projection is run where the typeinfos live, so it is shipped across the cluster and both it
     * and the type it returns have to be serializable.
     *
     * @param <T> The projected type.
     */
    @FunctionalInterface
    interface PVTypeInfoProjection<T> extends Serializable {
        T transform(String pvName, PVTypeInfo typeInfo);
    }

    /**
     * Look up the typeinfos for the named PVs and run the supplied projection operator over them.
     * This runs where the typeinfos are stored and can be called from any war file; only the projected
     * results come back, so project just the fields you need.
     * For example, to quickly determine the appliances for a bunch of PV's, project the applianceIdentity and then do a stream groupby.
     *
     * @param pvNames    The PV names to look up; names that are not being archived are absent from the result.
     * @param projection The projection to run against each matching typeinfo.
     * @return The projected results, in no particular order.
     */
    <T> Collection<T> queryPVTypeInfos(Collection<String> pvNames, PVTypeInfoProjection<T> projection);

    /**
     * Gets information about a PV's type, i.e its DBR type, graphic limits etc.
     * This information is assumed to be somewhat static and is expected to come from a cache if possible as it is used in data retrieval.
     *
     * @param pvName The name of PV.
     * @return PVTypeInfo  &emsp;
     */
    PVTypeInfo getTypeInfoForPV(String pvName);

    /**
     * Update the type information about a PV; updating both ther persistent and cached versions of the information.
     * Clients are not expected to call this method a million times a second.
     * In general, this is expected to be called when archiving a PV for the first time, or perhaps when an appserver startups etc...
     *
     * @param pvName   The name of PV.
     * @param typeInfo PVTypeInfo
     */
    void updateTypeInfoForPV(String pvName, PVTypeInfo typeInfo);

    /**
     * Remove the pv from all cached and persisted configuration.
     *
     * @param pvName The name of PV.
     */
    void removePVFromCluster(String pvName);
}
