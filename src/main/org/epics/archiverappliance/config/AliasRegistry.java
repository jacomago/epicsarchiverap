package org.epics.archiverappliance.config;

import java.util.List;

/**
 * Interface for managing aliases in the system.
 */
public interface AliasRegistry {
    /**
     * Register an alias
     *
     * @param aliasName &emsp;
     * @param realName  This is the name under which the PV will be archived under
     */
    void addAlias(String aliasName, String realName);

    /**
     * Remove an alias for the specified realname
     *
     * @param aliasName &emsp;
     * @param realName  This is the name under which the PV will be archived under
     */
    void removeAlias(String aliasName, String realName);

    /**
     * Get all the aliases in the system. This is used for matching during glob requests in the UI.
     *
     * @return String AllAliases &emsp;
     */
    List<String> getAllAliases();

    /**
     * Gets the .NAME field for a PV if it exists. Otherwise, this returns null
     *
     * @param aliasName &emsp;
     * @return String RealNameForAlias
     */
    String getRealNameForAlias(String aliasName);

    /**
     * Gets all the aliases in the system that map to this real PV name
     *
     * @param realName &emsp;
     * @return A list of aliases for this real name
     */
    List<String> getAliasesForRealName(String realName);
}
