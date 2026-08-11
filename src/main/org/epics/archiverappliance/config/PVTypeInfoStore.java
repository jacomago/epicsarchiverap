package org.epics.archiverappliance.config;

import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.Map;

/**
 * Interface for storing and retrieving PV type information.
 */
public interface PVTypeInfoStore {
    /**
     * Query this cluster's pvTypeInfos using the supplied the predicate and then run the supplied projection operator.
     * This runs using Hz's query functions and can be run from any war file
     * For example, to quickly determine the appliances for a bunch of PV's, project the applianceIdentity and then do a stream groupby.
     *
     * @return
     */
    <T> Collection<T> queryPVTypeInfos(
            Predicate<String, PVTypeInfo> predicate, Projection<Map.Entry<String, PVTypeInfo>, T> projection);

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
