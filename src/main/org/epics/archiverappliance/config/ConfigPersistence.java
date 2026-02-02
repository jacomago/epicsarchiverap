package org.epics.archiverappliance.config;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * Interface for persisting configuration
 * These are the pieces of configuration - all of these are key/value; keys are strings, values are typically JSON strings or plain strings.
 * <ol>
 * <li>Map&lt;String, PVTypeInfo&gt; typeInfos</li>
 * <li>Map&lt;String, UserSpecifiedSamplingParams&gt; archivePVRequests</li>
 * <li>Map&lt;String, String&gt; externalDataServer</li>
 * <li>Map&lt;String, String&gt; aliasNamesToRealNames</li>
 * </ol>
 * 
 * The APIs typically have one method to get all the keys, one to get a value given a key and a third to change a value given a key.
 * Others may be added later to improve performance and such.
 * @author mshankar
 *
 */
public interface ConfigPersistence {
	
	default void initialize(ConfigService configService) {
		
	}
	
	/**
	 * Get type info keys.
	 * @return Type info keys
	 * @throws IOException If fetch fails
	 */
	public List<String> getTypeInfoKeys() throws IOException;
	/**
	 * Get type info.
	 * @param pvName PV Name
	 * @return Type info
	 * @throws IOException If fetch fails
	 */
	public PVTypeInfo getTypeInfo(String pvName) throws IOException;
	/**
	 * Get all type infos for appliance.
	 * @param applianceIdentity Appliance Identity
	 * @return Type infos
	 * @throws IOException If fetch fails
	 */
	public List<PVTypeInfo> getAllTypeInfosForAppliance(String applianceIdentity) throws IOException;
	/**
	 * Put type info.
	 * @param pvName PV Name
	 * @param typeInfo Type info
	 * @throws IOException If put fails
	 */
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException;
	/**
	 * Delete type info.
	 * @param pvName PV Name
	 * @throws IOException If delete fails
	 */
	public void deleteTypeInfo(String pvName) throws IOException;
	
	
	/**
	 * Get archive PV requests keys.
	 * @return Archive PV requests keys
	 * @throws IOException If fetch fails
	 */
	public List<String> getArchivePVRequestsKeys() throws IOException;
	/**
	 * Get archive PV request.
	 * @param pvName PV Name
	 * @return Archive PV request
	 * @throws IOException If fetch fails
	 */
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException;
	/**
	 * Put archive PV request.
	 * @param pvName PV Name
	 * @param userParams User params
	 * @throws IOException If put fails
	 */
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException;
	/**
	 * Remove archive PV request.
	 * @param pvName PV Name
	 * @throws IOException If remove fails
	 */
	public void removeArchivePVRequest(String pvName) throws IOException;
	
	/**
	 * Get external data servers keys.
	 * @return External data servers keys
	 * @throws IOException If fetch fails
	 */
	public List<String> getExternalDataServersKeys() throws IOException;
	/**
	 * Get external data server.
	 * @param serverId Server ID
	 * @return External data server
	 * @throws IOException If fetch fails
	 */
	public String getExternalDataServer(String serverId) throws IOException;
	/**
	 * Put external data server.
	 * @param serverId Server ID
	 * @param serverInfo Server info
	 * @throws IOException If put fails
	 */
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException;
	/**
	 * Remove external data server.
	 * @param serverId Server ID
	 * @param serverInfo Server info
	 * @throws IOException If remove fails
	 */
	public void removeExternalDataServer(String serverId, String serverInfo) throws IOException;
	

	/**
	 * Get alias names to real names keys.
	 * @return Keys
	 * @throws IOException If fetch fails
	 */
	public List<String> getAliasNamesToRealNamesKeys() throws IOException;
	/**
	 * Get alias names to real name.
	 * @param pvName PV Name
	 * @return Real name
	 * @throws IOException If fetch fails
	 */
	public String getAliasNamesToRealName(String pvName) throws IOException;
	/**
	 * Get alias names for real name.
	 * @param realName Real name
	 * @return List of alias names
	 * @throws IOException If fetch fails
	 */
	default public List<String> getAliasNamesForRealName(String realName) throws IOException {
		LinkedList<String> ret = new LinkedList<String>();
		for(String aliasName : getAliasNamesToRealNamesKeys()) {
			if(getAliasNamesToRealName(aliasName).equals(realName)) {
				ret.add(aliasName);
			}
		}
		return ret;
	}
	/**
	 * Put alias names to real name.
	 * @param pvName PV Name
	 * @param realName Real name
	 * @throws IOException If put fails
	 */
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException;	
	/**
	 * Remove alias name.
	 * @param pvName PV Name
	 * @param realName Real name
	 * @throws IOException If remove fails
	 */
	public void removeAliasName(String pvName, String realName) throws IOException;	
}
