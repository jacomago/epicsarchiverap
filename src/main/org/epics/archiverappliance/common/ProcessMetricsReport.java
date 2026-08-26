package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.ProcessMetricsSource;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Combine the process metrics from the various wars and send as json.
 * @author mshankar
 *
 */
public class ProcessMetricsReport implements BPLAction {

    private final ApplianceLifecycle applianceLifecycle;
    private final ClusterTopology clusterTopology;
    private final ProcessMetricsSource processMetricsSource;

    public ProcessMetricsReport(
            ApplianceLifecycle applianceLifecycle,
            ClusterTopology clusterTopology,
            ProcessMetricsSource processMetricsSource) {
        this.applianceLifecycle = applianceLifecycle;
        this.clusterTopology = clusterTopology;
        this.processMetricsSource = processMetricsSource;
    }

    private static Logger logger = LogManager.getLogger(ProcessMetricsReport.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        ProcessMetrics processMetrics = processMetricsSource.getProcessMetrics();
        HashMap<String, Object> myProcessMetricsJSON = processMetrics.getProcessMetricsJSON(
                applianceLifecycle.getWarFile().name() + "_");
        if (applianceLifecycle.getWarFile() == ApplianceLifecycle.WAR_FILE.MGMT) {
            ApplianceInfo myApplianceInfo = clusterTopology.getMyApplianceInfo();
            // In this case we have combine the results from the various wars
            logger.debug("Asking for process metrics from engine using " + myApplianceInfo.getEngineURL()
                    + "/getProcessMetrics");
            JSONObject engineMetricsJSON =
                    GetUrlContent.getURLContentAsJSONObject(myApplianceInfo.getEngineURL() + "/getProcessMetrics");
            logger.debug(
                    "Asking for process metrics from ETL using " + myApplianceInfo.getEtlURL() + "/getProcessMetrics");
            JSONObject etlMetricsJSON =
                    GetUrlContent.getURLContentAsJSONObject(myApplianceInfo.getEtlURL() + "/getProcessMetrics");
            logger.debug("Asking for process metrics from retrieval using " + myApplianceInfo.getRetrievalURL()
                    + "/getProcessMetrics");
            JSONObject retrievalMetricsJSON =
                    GetUrlContent.getURLContentAsJSONObject(myApplianceInfo.getRetrievalURL() + "/getProcessMetrics");
            GetUrlContent.combineJSONObjectsWithArrays(myProcessMetricsJSON, engineMetricsJSON);
            GetUrlContent.combineJSONObjectsWithArrays(myProcessMetricsJSON, etlMetricsJSON);
            GetUrlContent.combineJSONObjectsWithArrays(myProcessMetricsJSON, retrievalMetricsJSON);
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONValue.writeJSONString(myProcessMetricsJSON, out);
        }
    }
}
