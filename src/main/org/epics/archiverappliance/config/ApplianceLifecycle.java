package org.epics.archiverappliance.config;

import com.google.common.eventbus.EventBus;
import org.epics.archiverappliance.config.exception.ConfigException;

import jakarta.servlet.ServletContext;

public interface ApplianceLifecycle {
    /**
     * This is the name used to identify the config service implementation in one of the existing singletons (like the ServletContext)
     */
    String CONFIG_SERVICE_NAME = "org.epics.archiverappliance.config.ConfigService";
    /**
     * This is an optional environment variable/system property that is used to identify the config service implementation.
     *
     */
    String ARCHAPPL_CONFIGSERVICE_IMPL = "ARCHAPPL_CONFIGSERVICE_IMPL";

    /**
     * If you have a null constructor and need a ServletContext, implement this method.
     *
     * @param sce ServletContext
     * @throws ConfigException &emsp;
     */
    void initialize(ServletContext sce) throws ConfigException;

    /**
     * We expect to live within a servlet container.
     * This call returns the full path to the WEB-INF folder of the webapp as it is deployed in the container.
     * Is typically a call to the <code>servletContext.getRealPath("WEB-INF/")</code>
     *
     * @return String WebInfFolder &emsp;
     */
    String getWebInfFolder();

    /**
     * This method is called after the mgmt WAR file has started up and set up the cluster and recovered data from persistence.
     * Each appliance's mgmt war is responsible for calling this method on the other components (engine, etl and retrieval) using BPL.
     * Until this method is called on all the web apps, the cluster is not considered to have started up.
     *
     * @throws ConfigException &emsp;
     */
    void postStartup() throws ConfigException;

    /**
     * Used for inter-appliance startup checks.
     *
     * @return STARTUP_SEQUENCE  &emsp;
     */
    ConfigService.STARTUP_SEQUENCE getStartupState();

    /**
     * Have we completed all the startup steps?
     *
     * @return boolean True or False
     */
    boolean isStartupComplete();

    /**
     * Get an approximate time in epoch seconds when the appserver started up.
     *
     * @return The time this app server started up.
     */
    long getTimeOfAppserverStartup();

    /**
     * Which component is this configservice instance.
     *
     * @return WAR_FILE  &emsp;
     */
    ConfigService.WAR_FILE getWarFile();

    /**
     * Is this appliance component shutting down?
     *
     * @return boolean True or False
     */
    boolean isShuttingDown();

    /**
     * Add an appserver agnostic shutdown hook; for example, to close the CA channels on shutdown
     *
     * @param runnable Runnable
     */
    void addShutdownHook(Runnable runnable);

    /**
     * Call the registered shutdown hooks and shut the archive appliance down.
     */
    void shutdownNow();

    /**
     * Get the event bus used for events within this appliance.
     *
     * @return EventBus &emsp;
     */
    EventBus getEventBus();
}
