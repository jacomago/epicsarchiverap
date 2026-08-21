package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.ProcessMetricsSource;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ProcessMetricsChartData implements BPLAction {

    private final ApplianceLifecycle applianceLifecycle;
    private final ClusterTopology clusterTopology;
    private final ProcessMetricsSource processMetricsSource;

    public ProcessMetricsChartData(
            ApplianceLifecycle applianceLifecycle,
            ClusterTopology clusterTopology,
            ProcessMetricsSource processMetricsSource) {
        this.applianceLifecycle = applianceLifecycle;
        this.clusterTopology = clusterTopology;
        this.processMetricsSource = processMetricsSource;
    }

    private static Logger logger = LogManager.getLogger(ProcessMetricsChartData.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String identity = req.getParameter("appliance");
        if (identity == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        if (!identity.equals(clusterTopology.getMyApplianceInfo().getIdentity())) {
            String url = clusterTopology.getAppliance(identity).getMgmtURL() + "/" + req.getPathInfo() + "?appliance="
                    + URLEncoder.encode(identity, "UTF-8");
            logger.debug("Proxying " + url);
            GetUrlContent.proxyURL(url, resp);
            return;
        }

        clusterTopology.getAppliance(identity);

        ProcessMetrics processMetrics = processMetricsSource.getProcessMetrics();
        assert (applianceLifecycle.getWarFile() == ApplianceLifecycle.WAR_FILE.MGMT);
        HashMap<String, Object> myProcessMetricsJSON = processMetrics.getProcessMetricsJSON(
                applianceLifecycle.getWarFile().name() + "_");

        ApplianceInfo myApplianceInfo = clusterTopology.getMyApplianceInfo();
        // In this case we have combine the results from the various wars
        logger.debug("Asking for process metrics from engine using " + myApplianceInfo.getEngineURL()
                + "/getProcessMetrics");
        JSONObject engineMetricsJSON =
                GetUrlContent.getURLContentAsJSONObject(myApplianceInfo.getEngineURL() + "/getProcessMetrics");
        logger.debug("Asking for process metrics from ETL using " + myApplianceInfo.getEtlURL() + "/getProcessMetrics");
        JSONObject etlMetricsJSON =
                GetUrlContent.getURLContentAsJSONObject(myApplianceInfo.getEtlURL() + "/getProcessMetrics");
        logger.debug("Asking for process metrics from retrieval using " + myApplianceInfo.getRetrievalURL()
                + "/getProcessMetrics");
        JSONObject retrievalMetricsJSON =
                GetUrlContent.getURLContentAsJSONObject(myApplianceInfo.getRetrievalURL() + "/getProcessMetrics");
        GetUrlContent.combineJSONObjectsWithArrays(myProcessMetricsJSON, engineMetricsJSON);
        GetUrlContent.combineJSONObjectsWithArrays(myProcessMetricsJSON, etlMetricsJSON);
        GetUrlContent.combineJSONObjectsWithArrays(myProcessMetricsJSON, retrievalMetricsJSON);

        // In the mgmt webapp, we convert this into a jflot friendly json, something like so....
        // [
        //    { label: "engine_heap", data: [ [22414837000, 25.16], [22414838000, 26.82] ] },
        //    { label: "system_load", data: [ [22414837000, 0.36],  [22414838000, 0.13] ] }
        // ]

        try {
            LinkedList<Object> finalData = new LinkedList<Object>();
            addChartData(myProcessMetricsJSON, finalData, "ENGINE_metrics", "systemLoadAverage", "system_load (%)");
            addChartData(myProcessMetricsJSON, finalData, "ENGINE_metrics", "heapUsedPercent", "engine_heap (%)");
            addChartData(myProcessMetricsJSON, finalData, "ETL_metrics", "heapUsedPercent", "etl_heap (%)");
            addChartData(myProcessMetricsJSON, finalData, "RETRIEVAL_metrics", "heapUsedPercent", "retrieval_heap (%)");
            // addChartData(myProcessMetricsJSON, finalData, "MGMT_metrics", "heapUsedPercent", "mgmt_heap");
            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            try (PrintWriter out = resp.getWriter()) {
                JSONValue.writeJSONString(finalData, out);
            }
        } catch (Exception ex) {
            logger.error(ex);
            throw new IOException(ex);
        }
    }

    private static void addChartData(
            HashMap<String, Object> myProcessMetricsJSON,
            LinkedList<Object> finalData,
            String keyInProcessMetrics,
            String yaxisdataelementName,
            String chartlabel) {
        JSONArray processMetrics = (JSONArray) myProcessMetricsJSON.get(keyInProcessMetrics);
        if (processMetrics != null && processMetrics.size() > 0) {
            HashMap<String, Object> processMetric = new HashMap<String, Object>();
            processMetric.put("label", chartlabel);
            LinkedList<Object> data = new LinkedList<Object>();
            for (Object metric : processMetrics) {
                @SuppressWarnings("unchecked")
                HashMap<String, String> engineMetric = (HashMap<String, String>) metric;
                String timeInEpochSecondsStr = engineMetric.get("timeInEpochSeconds");
                long epochMillisUTC = Long.parseLong(timeInEpochSecondsStr) * 1000;
                long epochMillisLocalTZ = TimeUtils.convertToLocalEpochMillis(epochMillisUTC);
                String heapUsedPercentStr = engineMetric.get(yaxisdataelementName);
                Double heapUsedPercent = Double.parseDouble(heapUsedPercentStr);
                LinkedList<Number> dataElements = new LinkedList<Number>();
                dataElements.add(epochMillisLocalTZ);
                dataElements.add(heapUsedPercent);
                data.add(dataElements);
            }
            processMetric.put("data", data);
            finalData.add(processMetric);
        }
    }
}
