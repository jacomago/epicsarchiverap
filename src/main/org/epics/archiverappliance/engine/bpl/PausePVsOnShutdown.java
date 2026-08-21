package org.epics.archiverappliance.engine.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.PolicyService;
import org.epics.archiverappliance.config.StoragePluginConfigView;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Pause all PVs; this does not update PVTypeinfo.
 * This is an internal call that is typically to be used only on shutdown.
 * @author mshankar
 *
 */
public class PausePVsOnShutdown implements BPLAction {

    private final StoragePluginConfigView storageConfig;
    private final ApplianceLifecycle applianceLifecycle;
    private final PolicyService policyService;

    public PausePVsOnShutdown(
            StoragePluginConfigView storageConfig, ApplianceLifecycle applianceLifecycle, PolicyService policyService) {
        this.storageConfig = storageConfig;
        this.applianceLifecycle = applianceLifecycle;
        this.policyService = policyService;
    }

    private static Logger configlogger = LogManager.getLogger("config." + PausePVsOnShutdown.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        configlogger.info("Pausing PVs on potential shutdown");
        EngineContext engineRuntime = EngineContext.of(applianceLifecycle);
        int pvCount = 0;
        for (String pvName : engineRuntime.getChannelList().keySet()) {
            try {
                ArchiveEngine.pauseArchivingPV(pvName, storageConfig, applianceLifecycle, policyService);
                pvCount++;
            } catch (Exception ex) {
                configlogger.error("Exception pausing PV " + pvName, ex);
            }
        }

        HashMap<String, Object> infoValues = new HashMap<String, Object>();
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            infoValues.put("status", "ok");
            infoValues.put("desc", "Successfully paused " + pvCount + " pvs");
            out.println(JSONValue.toJSONString(infoValues));
        }
    }
}
