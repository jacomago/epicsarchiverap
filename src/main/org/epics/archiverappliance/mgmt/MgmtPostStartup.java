package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Make sure all the other web apps have started and if so, send them their post startup messages...
 * @author mshankar
 *
 */
public class MgmtPostStartup implements Runnable {
    private static Logger logger = LogManager.getLogger(MgmtPostStartup.class.getName());
    private static Logger configlogger = LogManager.getLogger("config." + MgmtPostStartup.class.getName());
    private ScheduledFuture<?> cancellingFuture;
    private final ApplianceLifecycle applianceLifecycle;
    private final ClusterTopology clusterTopology;

    public MgmtPostStartup(ApplianceLifecycle applianceLifecycle, ClusterTopology clusterTopology) {
        this.applianceLifecycle = applianceLifecycle;
        this.clusterTopology = clusterTopology;
    }

    @Override
    public void run() {
        logger.info("About to run MgmtPostStartup");
        if (this.applianceLifecycle.isStartupComplete()) {
            logger.info("Startup is complete for MgmtPostStartup");
            if (MgmtRuntimeState.of(this.applianceLifecycle).haveChildComponentsStartedUp()) {
                cancellingFuture.cancel(false);
            } else {
                this.checkIfAllComponentsHaveStartedUp();
                if (MgmtRuntimeState.of(this.applianceLifecycle).haveChildComponentsStartedUp()) {
                    cancellingFuture.cancel(false);
                }
            }
        } else {
            try {
                logger.debug("Before post startup in MgmtPostStartup");
                applianceLifecycle.postStartup();
                configlogger.info("Finished post startup for the mgmt webapp");
            } catch (ConfigException ex) {
                logger.error("Exception running post startup on the management app", ex);
            }
        }
    }

    public void setCancellingFuture(ScheduledFuture<?> cancellingFuture) {
        this.cancellingFuture = cancellingFuture;
    }

    private void checkIfAllComponentsHaveStartedUp() {
        // Check to see the other apps to see if the mgmt webapp is starting up after a JVM crash.
        // In normal circumstances, the other apps should start up and go thru the webappReady/postStartup exchange
        // However, in case the mgmt webapp crashes and is restarted by jsvc, we need to mimic the startup sequence...
        try {
            ApplianceInfo myApplianceInfo = clusterTopology.getMyApplianceInfo();
            {
                logger.debug("Asking for startup status from the retrieval web app");
                String url = myApplianceInfo.getRetrievalURL() + "/startupState";
                @SuppressWarnings("unchecked")
                HashMap<String, String> retrievalStatus =
                        (HashMap<String, String>) GetUrlContent.getURLContentAsJSONObject(url);
                ApplianceLifecycle.STARTUP_SEQUENCE retrievalStartupState =
                        ApplianceLifecycle.STARTUP_SEQUENCE.valueOf(retrievalStatus.get("status"));
                if (retrievalStartupState == ApplianceLifecycle.STARTUP_SEQUENCE.STARTUP_COMPLETE) {
                    MgmtRuntimeState.of(applianceLifecycle).componentStartedUp(ApplianceLifecycle.WAR_FILE.RETRIEVAL);
                }
            }

            {
                logger.debug("Asking for startup status from the ETL web app");
                String url = myApplianceInfo.getEtlURL() + "/startupState";
                @SuppressWarnings("unchecked")
                HashMap<String, String> etlStatus =
                        (HashMap<String, String>) GetUrlContent.getURLContentAsJSONObject(url);
                ApplianceLifecycle.STARTUP_SEQUENCE etlStartupState =
                        ApplianceLifecycle.STARTUP_SEQUENCE.valueOf(etlStatus.get("status"));
                if (etlStartupState == ApplianceLifecycle.STARTUP_SEQUENCE.STARTUP_COMPLETE) {
                    MgmtRuntimeState.of(applianceLifecycle).componentStartedUp(ApplianceLifecycle.WAR_FILE.ETL);
                }
            }

            {
                logger.debug("Asking for startup status from the engine web app");
                String url = myApplianceInfo.getEngineURL() + "/startupState";
                @SuppressWarnings("unchecked")
                HashMap<String, String> engineStatus =
                        (HashMap<String, String>) GetUrlContent.getURLContentAsJSONObject(url);
                ApplianceLifecycle.STARTUP_SEQUENCE engineStartupState =
                        ApplianceLifecycle.STARTUP_SEQUENCE.valueOf(engineStatus.get("status"));
                if (engineStartupState == ApplianceLifecycle.STARTUP_SEQUENCE.STARTUP_COMPLETE) {
                    MgmtRuntimeState.of(applianceLifecycle).componentStartedUp(ApplianceLifecycle.WAR_FILE.ENGINE);
                }
            }

        } catch (Exception ex) {
            logger.warn("Exception checking startup state", ex);
        }
    }
}
