package org.epics.archiverappliance.config;

import org.epics.archiverappliance.mgmt.policy.PolicyConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Interface for managing the policy calculation for PVs in the system.
 */
public interface PolicyService {
    /**
     * This is an optional environment/system.property that is used to specify the location of the <code>policies.py</code> policies file.
     * If this is not set, we search for the  <code>policies.py</code> in the servlet classpath using the path <code>/WEB-INF/classes/policies.py</code>
     */
    String ARCHAPPL_POLICIES = "ARCHAPPL_POLICIES";

    /**
     * Return the text of the policy for this installation.
     * Gets you an InputStream; remember to close it.
     *
     * @return InputStream  &emsp;
     * @throws IOException &emsp;
     */
    InputStream getPolicyText() throws IOException;

    /**
     * Given a pvName (for now, we should have a pv details object of some kind soon), determine the policy applicable for archiving this PV.
     *
     * @param pvName         The name of PV.
     * @param metaInfo       The MetaInfo of PV
     * @param userSpecParams UserSpecifiedSamplingParams
     * @return PolicyConfig  &emsp;
     * @throws IOException &emsp;
     */
    PolicyConfig computePolicyForPV(String pvName, MetaInfo metaInfo, UserSpecifiedSamplingParams userSpecParams)
            throws IOException;

    /**
     * Return a map of name to description of all the policies in the system
     * This is used to drive a dropdown in the UI.
     *
     * @return HashMap  &emsp;
     * @throws IOException &emsp;
     */
    HashMap<String, String> getPoliciesInInstallation() throws IOException;

    /**
     * Get a list of extra fields that are obtained when we initially make a request for archiving.
     * These are used in the policies to make decisions on how to archive the PV.
     * @return String ExtraFields  &emsp;
     */
    public String[] getExtraFields();

    /**
     * Get a list of fields for PVs that are monitored and maintained in the engine.
     * These are used when displaying the PV in visualization tools like the ArchiveViewer as additional information for the PV.
     * Some of these could be archived along with the PV but need not be.
     * In this case, the engine simply maintains the latest copy in memory and this is served up when data from the engine in included in the stream.
     * @return String RuntimeFields
     */
    public Set<String> getRuntimeFields();

    /**
     * This product offers the ability to archive certain fields (like HIHI, LOLO etc) as part of every PV.
     * The data for these fields is embedded into the stream as extra fields using the FieldValues interface of events.
     * This method lists all these fields.
     * Requests for archiving these fields are deferred to and combined with the request for archiving the .VAL.
     * We also assume that the data type (double/float) for these fields is the same as the .VAL.
     * @return String  &emsp;
     * @throws IOException  &emsp;
     */
    public List<String> getFieldsArchivedAsPartOfStream() throws IOException;
}
