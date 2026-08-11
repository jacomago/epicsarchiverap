package org.epics.archiverappliance.config;

public interface ClusterTopology {
    /**
     * This is the environment variable that points to the file containing the various appliances in this cluster.
     * This list of appliances is expected to be the same for all appliances in the cluster; so it is perfectly legal to place it in NFS somewhere and point to the same file/location from all appliances in the cluster.
     * It is reasonably important that all appliances see the same list of cluster members or we tend to have split-brain effects (<a href="http://en.wikipedia.org/wiki/Split-brain_%28computing%29">See wikipedia</a>).
     * The format of the file itself is simple XML like so
     * <pre>
     * &lt;appliances&gt;
     *   &lt;appliance&gt;
     *     &lt;identity&gt;archiver&lt;/identity&gt;
     *     &lt;cluster_inetport&gt;archiver:77770&lt;/cluster_inetport&gt;
     *     &lt;mgmt_url&gt;http://archiver.slac.stanford.edu:77765/mgmt/bpl&lt;/mgmt_url&gt;
     *     &lt;engine_url&gt;http://archiver.slac.stanford.edu:77765/engine/bpl&lt;/engine_url&gt;
     *     &lt;etl_url&gt;http://archiver.slac.stanford.edu:77765/etl/bpl&lt;/etl_url&gt;
     *     &lt;retrieval_url&gt;http://archiver.slac.stanford.edu:77765/retrieval/bpl&lt;/retrieval_url&gt;
     *     &lt;data_retrieval_url&gt;http://archiver.slac.stanford.edu:77765/retrieval/&lt;/data_retrieval_url&gt;
     *   &lt;/appliance&gt;
     * &lt;/appliances&gt;
     * </pre>
     * Note that the appliance identity as defined by the <code>ARCHAPPL_MYIDENTITY</code> has to match the <code>identity</code> element of one of the appliances in the list of appliances as defined by the <code>ARCHAPPL_APPLIANCES</code>.
     * Each appliance (which includes the mgmt, engine, etl and retrieval WAR's) must have a unique identity.
     * <br>
     * If the <code>ARCHAPPL_APPLIANCES</code> is not set, then we look for a file called <code>appliances.xml</code> in the WEB-INF/classes of the current WAR using WEB-INF/classes/appliances.xml.
     * The default build script places the site-specific <code>appliances.xml</code> into WEB-INF/classes/appliances.xml.
     */
    String ARCHAPPL_APPLIANCES = "ARCHAPPL_APPLIANCES";
    /**
     * This is an optional environment variable that determines this appliance's identity.
     * If this is not set, the archiver appliance uses <code>InetAddress.getLocalHost().getCanonicalHostName()</code> to determine the FQDN of this machine.
     * This is then used as the appliance identity to lookup the appliance info in <code>ARCHAPPL_APPLIANCES</code>.
     * To use this environment variable, for example, in Linux, set the appliance's identity using <code>export ARCHAPPL_MYIDENTITY="archiver"</code>.
     * Each appliance (which includes the mgmt, engine, etl and retrieval WAR's) must have a unique identity.
     * <p>
     * To accommodate the multi-instance unit tests, if this environment variable is not set, we check for the existence of the java system property <code>ARCHAPPL_MYIDENTITY</code>.
     * Typically, the multi-instance unit tests (which are incapable of altering the environment) use the java system property method.
     * In environments that run the unit tests, leave the environment variable ARCHAPPL_MYIDENTITY unset so that the various multi-instance unit tests have the ability to control the appliance identity.
     */
    String ARCHAPPL_MYIDENTITY = "ARCHAPPL_MYIDENTITY";

    /**
     * Get all the appliances in this cluster.
     * Much goodness is facilitated if the objects are returned in the same order (perhaps order of creation) all the time.
     *
     * @return ApplianceInfo &emsp;
     */
    Iterable<ApplianceInfo> getAppliancesInCluster();

    /**
     * Get the appliance information for this appliance.
     *
     * @return ApplianceInfo  &emsp;
     */
    ApplianceInfo getMyApplianceInfo();

    /**
     * Given an identity of an appliance, return the appliance info for that appliance
     *
     * @param identity The appliance identify
     * @return ApplianceInfo &emsp;
     */
    ApplianceInfo getAppliance(String identity);

    /**
     * To prevent split brain side-effects, we support cetain BPL only when all the member of the cluster have finished loading their PVs into the cluster.
     * This consists of two checks
     * 1) Make sure all appliances listed in appliances.xml have started up and are part of the cluster
     * 2) All appliances in the cluster have registered their PVs with the cluster.
     * <p>
     * Previously, we'd allow appliances.xml to have more appliances that are actually present in the cluster.
     * However; this is becoming increasingly hard to support.
     * We've had to tighten this to avoid split brain issues which can happen when the networking between instances fails.
     *
     * @return boolean &emsp;
     */
    boolean hasClusterFinishedInitialization();
}
