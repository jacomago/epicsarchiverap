package org.epics.archiverappliance.config;

/**
 * Interface for configuring the naming conventions for PVs in the system.
 */
public interface PVNamingConfig {
    /**
     * Returns a TypeSystem object that is used to convert from JCA DBR's to Event's (actually, DBRTimeEvents)
     *
     * @return TypeSystem  &emsp;
     */
    TypeSystem getArchiverTypeSystem();

    /**
     * Implementation for converting a PV name to something that forms the prefix of a chunk's key.
     * See @see{PVNameToKeyMapping} for more details.
     *
     * @return PVNameToKeyMapping  &emsp;
     */
    PVNameToKeyMapping getPVNameToKeyConverter();
}
