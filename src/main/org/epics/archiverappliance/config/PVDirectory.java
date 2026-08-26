package org.epics.archiverappliance.config;

import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface PVDirectory {
    /**
     * Get an exhaustive list of all the PVs this cluster of appliances knows about
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     *
     * @return String AllPVs &emsp;
     */
    Collection<String> getAllPVs();

    /**
     * For automated PV submission, IOC engineers could add .VAL, fields, aliases etc.
     * This method attempts to return all possible PV's that the archiver could know about.
     * This is a lot of names; so we take in a consumer that potentially streams a name out as quickly as possible.
     *
     * @param func A consumer of pvNames
     */
    void getAllExpandedNames(Consumer<String> func);

    /**
     * Given a PV, get us the appliance that is responsible for archiving it.
     * Note that this may be null as the assignment of PV's to appliances can take some time.
     *
     * @param pvName The name of PV.
     * @return ApplianceInfo &emsp;
     */
    ApplianceInfo getApplianceForPV(String pvName);

    /**
     * Get all PVs being archived by this appliance.
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     *
     * @param info ApplianceInfo
     * @return string All PVs being archiveed by this appliance
     */
    Set<String> getPVsForAppliance(ApplianceInfo info);

    /**
     * Get all the PVs for this appliance.
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     *
     * @return String All PVs being archiveed for this appliance
     */
    Set<String> getPVsForThisAppliance();

    /*
     * For performance reasons, we cache the total PV count and the paused PV count for this appliance.
     */
    CachedPVCounts getCachedPVCountsForThisAppliance();

    /**
     * Prepare for batch jobs by breaking down a list of PV's into a Map that maps appliance identity to a list of PV's being archived on that appliance.
     *
     * @return
     */
    Map<String, List<String>> breakDownPVsByAppliance(List<String> pvNames);

    /**
     * Get the pvNames for this appliance matching the given regex.
     *
     * @param nameToMatch &emsp;
     * @return string PVsForApplianceMatchingRegex   &emsp;
     */
    Set<String> getPVsForApplianceMatchingRegex(String nameToMatch);

    /**
     * Make changes in the config service to register this PV to an appliance
     *
     * @param pvName           The name of PV.
     * @param applianceInfo    ApplianceInfo
     * @param registrationType If reassigning; then an AlreadyRegisteredException is not raised
     * @throws AlreadyRegisteredException &emsp;
     */
    void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo, PVRegistrationType registrationType)
            throws AlreadyRegisteredException;

    default void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo) throws AlreadyRegisteredException {
        this.registerPVToAppliance(pvName, applianceInfo, PVRegistrationType.ARCHIVNG);
    }

    /**
     * Facilitates various optimizations for BPL that uses appliance wide information by caching and maintaining this information on a per appliance basis
     *
     * @param applianceInfo ApplianceInfo
     * @return ApplianceAggregateInfo  &emsp;
     * @throws IOException &emsp;
     */
    ApplianceAggregateInfo getAggregatedApplianceInfo(ApplianceInfo applianceInfo) throws IOException;

    /**
     * Get a set of PVs that have been paused in this appliance.
     *
     * @return String  &emsp;
     */
    Set<String> getPausedPVsInThisAppliance();

    record CachedPVCounts(int totalPVCount, int pausedPVCount) implements Serializable {}
}
