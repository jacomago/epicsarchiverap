package org.epics.archiverappliance.config;

import java.util.Set;

/**
 * Interface for configuring failover appliances.
 */
public interface FailoverConfig {
    /**
     * Get a list of external archiver appliances configured for failover.
     * Failover appliances are not used as proxies to minimize imposing the retrieval load of the this installation on a potentially less powerful appliance used principally for failover.
     *
     * @return
     */
    Set<String> getFailoverServerURLs();

    /**
     * Get the first external archiver appliance that also archives this PV and is configured with a mergeDuringRetrieval query parameter.
     *
     * @return - Returns null if no failover appliance.
     */
    String getFailoverApplianceURL(String pvName);

    /**
     * Each retrieval component in a cluster caches the PV's from remote failover appliances.
     * These caches contain one entry for each PV in this appliance indicating if the PV is being archived in the remote appliance.
     * This information is cached using a TTL to minimize the impact on the remote failover appliance.
     * This method manually unloads this cache.
     */
    void resetFailoverCaches();
}
