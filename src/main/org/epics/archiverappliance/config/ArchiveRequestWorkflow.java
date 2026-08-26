package org.epics.archiverappliance.config;

import java.util.Set;

public interface ArchiveRequestWorkflow {
    /**
     * The workflow for requesting a PV to be archived consists of multiple steps
     * This method adds a PV to the persisted list of PVs that are currently engaged in this workflow in addition to any user specified overrides
     *
     * @param pvName                      The name of PV.
     * @param userSpecifiedSamplingParams - Use a null contructor for userSpecifiedSamplingParams if no override specified.
     */
    void addToArchiveRequests(String pvName, UserSpecifiedSamplingParams userSpecifiedSamplingParams);

    /**
     * Update the archive request (mostly with aliases) if and only if we have this in our persistence.
     *
     * @param pvName                      The name of PV.
     * @param userSpecifiedSamplingParams &emsp;
     */
    void updateArchiveRequest(String pvName, UserSpecifiedSamplingParams userSpecifiedSamplingParams);

    /**
     * Gets a list of PVs that are currently engaged in the archive PV workflow
     *
     * @return String ArchiveRequestsCurrentlyInWorkflow  &emsp;
     */
    Set<String> getArchiveRequestsCurrentlyInWorkflow();

    /**
     * Is this pv in the archive request workflow.
     *
     * @param pvname The name of PV.
     * @return boolean True or False
     */
    boolean doesPVHaveArchiveRequestInWorkflow(String pvname);

    /**
     * In clustered environments, to give capacity planning a chance to work correctly, we want to kick off the archive PV workflow only after all the machines have started.
     * This is an approximation for that metric; though not a very satisfactory approximation.
     * TODO -- Think thru implications of making the appliances.xml strict...
     *
     * @return - Initial delay in seconds.
     */
    int getInitialDelayBeforeStartingArchiveRequestWorkflow();

    /**
     * Returns any user specified parameters for the archive request.
     *
     * @param pvName The name of PV.
     * @return UserSpecifiedSamplingParams  &emsp;
     */
    UserSpecifiedSamplingParams getUserSpecifiedSamplingParams(String pvName);

    /**
     * Mark this pv as having it archive pv request completed and pull this request out of persistent store
     * Can be used in the case of aborting a PV archive request as well
     *
     * @param pvName The name of PV.
     */
    void archiveRequestWorkflowCompleted(String pvName);
}
