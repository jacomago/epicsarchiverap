package org.epics.archiverappliance.config.persistence;

import org.epics.archiverappliance.config.ConfigPersistence;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dummy in memory persistence layer for unit tests
 * @author mshankar
 *
 */
public class InMemoryPersistence implements ConfigPersistence {
    private final ConcurrentHashMap<String, PVTypeInfo> typeInfos = new ConcurrentHashMap<String, PVTypeInfo>();
    private final ConcurrentHashMap<String, UserSpecifiedSamplingParams> archivePVRequests =
            new ConcurrentHashMap<String, UserSpecifiedSamplingParams>();
    private final ConcurrentHashMap<String, String> externalDataServersKeys = new ConcurrentHashMap<String, String>();
    private final ConcurrentHashMap<String, String> aliasNamesToRealNames = new ConcurrentHashMap<String, String>();

    @Override
    public List<String> getTypeInfoKeys() throws IOException {
        return new LinkedList<>(typeInfos.keySet());
    }

    @Override
    public List<PVTypeInfo> getAllTypeInfosForAppliance(String applianceIdentity) throws IOException {
        return new LinkedList<>(typeInfos.values());
    }

    @Override
    public PVTypeInfo getTypeInfo(String pvName) throws IOException {
        return typeInfos.get(pvName);
    }

    @Override
    public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
        typeInfos.put(pvName, typeInfo);
    }

    @Override
    public void deleteTypeInfo(String pvName) {
        typeInfos.remove(pvName);
    }

    @Override
    public List<String> getArchivePVRequestsKeys() {
        return new LinkedList<>(archivePVRequests.keySet());
    }

    @Override
    public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) {
        return archivePVRequests.get(pvName);
    }

    @Override
    public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) {
        archivePVRequests.put(pvName, userParams);
    }

    @Override
    public void removeArchivePVRequest(String pvName) {
        archivePVRequests.remove(pvName);
    }

    @Override
    public List<String> getExternalDataServersKeys() throws IOException {
        return new LinkedList<>(externalDataServersKeys.keySet());
    }

    @Override
    public String getExternalDataServer(String serverId) throws IOException {
        return externalDataServersKeys.get(serverId);
    }

    @Override
    public void putExternalDataServer(String serverId, String serverInfo) throws IOException {
        externalDataServersKeys.put(serverId, serverInfo);
    }

    @Override
    public void removeExternalDataServer(String serverId, String serverInfo) throws IOException {
        externalDataServersKeys.remove(serverId);
    }

    @Override
    public List<String> getAliasNamesToRealNamesKeys() throws IOException {
        return new LinkedList<>(aliasNamesToRealNames.keySet());
    }

    @Override
    public String getAliasNamesToRealName(String pvName) throws IOException {
        return aliasNamesToRealNames.get(pvName);
    }

    @Override
    public void putAliasNamesToRealName(String pvName, String realName) throws IOException {
        aliasNamesToRealNames.put(pvName, realName);
    }

    @Override
    public void removeAliasName(String pvName, String realName) throws IOException {
        aliasNamesToRealNames.remove(pvName);
    }
}
