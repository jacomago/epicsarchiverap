package org.epics.archiverappliance.config;

import java.util.Properties;
import java.util.Set;

public interface InstallationProperties {

    /**
     * The name/path of the archappl.properties file.
     * By default, we look for archappl.properties in the webapp's classpath - this will typically resolve into WEB-INF/classes of the webapp.
     * However, you can override this using an environment variable (or java system property) of the same name.
     * For example, <code>export ARCHAPPL_PROPERTIES_FILENAME=/etc/mylab_archappl.properties</code> should force the components to load their properties from <code>/etc/mylab_archappl.properties</code>
     */
    String ARCHAPPL_PROPERTIES_FILENAME = "ARCHAPPL_PROPERTIES_FILENAME";

    /**
     * This is the name of the properties file that is looked for in the webapp's classpath if one is not specified using a environment/JVM property.
     */
    String DEFAULT_ARCHAPPL_PROPERTIES_FILENAME = "archappl.properties";

    String ARCHAPPL_NAMEDFLAGS_PROPERTIES_FILE_PROPERTY = "org.epics.archiverappliance.config.NamedFlags.readFromFile";

    /**
     * An arbitrary list of name/value pairs can be specified in a file called archappl.properties that is loaded from the classpath.
     *
     * @return Properties &emsp;
     */
    Properties getInstallationProperties();

    /**
     * Named flags are used to control various process in the appliance; for example, the ETL process in a PlainStoragePlugin
     * Named flags are not persistent; each time the server starts up, all the named flags are set to false
     * You can optionally load values for named flags from a file by specifying the ARCHAPPL_NAMEDFLAGS_PROPERTIES_FILE_PROPERTY property in archappl.properties.
     * This method gets the value of the specified named flag.
     * If the flag has not been defined before in the cluster, this method will return false.
     * @param name  &emsp;
     * @return boolean True or False
     */
    public boolean getNamedFlag(String name);

    /**
     * Sets the value of the named flag specified by name to the specified value
     * @param name  &emsp;
     * @param value   &emsp;
     */
    public void setNamedFlag(String name, boolean value);

    /**
     * Return the names of all the named flags that we know about
     * @return String  &emsp;
     */
    public Set<String> getNamedFlagNames();
}
