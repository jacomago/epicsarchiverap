package org.epics.archiverappliance.config;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for configuring the external Channel Archiver sources.
 */
public interface ChannelArchiverConfig {
    /**
     * This product has the ability to proxy data from other archiver data servers.
     * We currently integrate with Channel Archiver XMLRPC data servers and other EPICS Archiver Appliance clusters.
     * Get a list of external Archiver Data Servers that we know about.
     *
     * @return Map ExternalArchiverDataServers
     */
    Map<String, String> getExternalArchiverDataServers();

    /**
     * Add a external Archiver Data Server into the system.
     *
     * @param serverURL   - For Channel Archivers, this is the URL to the XML-RPC server. For other EPICS Archiver Appliance clusters, this is the <code>data_retrieval_url</code> of the cluster as defined in the <code>appliances.xml</code>.
     * @param archivesCSV - For Channel Archivers, this is a comma separated list of indexes. For other EPICS Archiver Appliance clusters, this is the string <i>pbraw</i>.
     * @throws IOException &emsp;
     */
    void addExternalArchiverDataServer(String serverURL, String archivesCSV) throws IOException;

    /**
     * Removes an entry for an external Archiver Data Server from the system
     * Note; we may need to restart the entire cluster for this change to take effect.
     *
     * @param serverURL   - For Channel Archivers, this is the URL to the XML-RPC server. For other EPICS Archiver Appliance clusters, this is the <code>data_retrieval_url</code> of the cluster as defined in the <code>appliances.xml</code>.
     * @param archivesCSV - For Channel Archivers, this is a comma separated list of indexes. For other EPICS Archiver Appliance clusters, this is the string <i>pbraw</i>.
     * @throws IOException &emsp;
     */
    void removeExternalArchiverDataServer(String serverURL, String archivesCSV) throws IOException;

    /**
     * Return a list of ChannelArchiverDataServerPVInfos for a PV if one exists; otherwise return null.
     * The servers are sorted in order of the start seconds.
     * Note: this only applies to Channel Archiver XML RPC servers.
     * For proxying external EPICS Archiver Appliance clusters, we do not cache the PV's that are being archived on the external system.
     *
     * @param pvName The name of PV.
     * @return ChannelArchiverDataServerPVInfo  &emsp;
     */
    List<ChannelArchiverDataServerPVInfo> getChannelArchiverDataServers(String pvName);

    /**
     * For all the Channel Archiver XMLRPC data servers in the mix, update the PV info.
     * This should help improve performance a little in proxying data from ChannelArchiver data servers that are still active.
     * For proxying external EPICS Archiver Appliance clusters, since we do not cache the PV's that are being archived on the external system, this is a no-op.
     */
    void refreshPVDataFromChannelArchiverDataServers();
}
