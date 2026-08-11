package org.epics.archiverappliance.config;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface for executing operations across a cluster of appliances.
 */
public interface ClusterExecutor {
    /*
     * Execute the specified EAABulkOperation on all active members in the cluster.
     * Gather the results into a hashmap indexed by appliance identity.
     * In a typical usecase, we first breakdown a list of PVs into a per appliance Map ( String -> List<String> ) using breakDownPVsByAppliance.
     * We send this entire Map to all the active members in the cluster.
     * The operation can then use the appliance identity from the configservice to determine which subset of PVs are applicable
     * to this instance and perform the appropriate operation on this subset.
     * The results are returned in a Map ( Appliance Identity -> Result ) and callee is then expected to merge the results appropriately
     * This is mainly intended for quick turn around operations that change state ( like changing the pause/resume status for example )
     * One can easily overload the HZ executor so maybe we should not use it for bulk renaming/resharding or any other operation that take a significant amount of time.
     * But small changes to PVTypeInfo's in bulk are the intended usecase.
     */
    <T> Map<String, T> executeClusterWide(EAABulkOperation<T> theOperation);

    /*
     * Same as above but only on specified appliance.
     */
    <T> T executeOnAppliance(ApplianceInfo applianceInfo, EAABulkOperation<T> theOperation);

    /*
     * Like a callable but for bulk operations within the EAA cluster.
     * The EAABulkOperation is serialized and send to all ( active ) mgmt members in the cluster using the Hz Executor service.
     *
     */
    public interface EAABulkOperation<T> extends Serializable {
        public T call(ConfigService configService);
    }
}
