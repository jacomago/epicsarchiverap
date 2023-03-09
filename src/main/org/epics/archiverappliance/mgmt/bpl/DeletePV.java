package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Stop archiving the specified PV. The PV needs to be paused first.
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionParam deleteData - Should we delete the data that has already been recorded. Optional, by default, we do not delete the data for this PV. Can be <code>true</code> or <code>false</code>.
 * @epics.BPLActionEnd
 *
 * @author mshankar
 *
 */
public class DeletePV implements BPLAction {
    public static final String BAD_REQUEST = "Bad request";
    private static final Logger logger = LogManager.getLogger(DeletePV.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        if (req.getMethod().equals("POST")) {
            deleteMultiplePVs(req, resp, configService);
            return;
        }

        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (pvName.contains(",") || pvName.contains("*") || pvName.contains("?")) {
            deleteMultiplePVs(req, resp, configService);
        } else {
            // We only have one PV in the request
            String strValues = deleteSinglePV(req, resp, configService, null);
            HashMap<String, Object> infoValues = new HashMap<String, Object>();
            infoValues.put("validation", strValues);
            if (strValues.isEmpty()) infoValues.put("status", "ok");
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(infoValues));
            }
        }
    }

    private static JSONObject instanceDeletePV(
            HashMap<String, String> pvStatus, String engineStart, String info, String x, String engineEnd) {
        pvStatus.put(engineStart, TimeUtils.convertToHumanReadableString(System.currentTimeMillis() / 1000));

        logger.info(() -> x + info);
        JSONObject pvEngineStatus = GetUrlContent.getURLContentAsJSONObject(info);
        pvStatus.put(engineEnd, TimeUtils.convertToHumanReadableString(System.currentTimeMillis() / 1000));
        return pvEngineStatus;
    }

    private String deleteSinglePV(
            HttpServletRequest req, HttpServletResponse resp, ConfigService configService, String pvName)
            throws IOException {
        String infoValues = "";
        if (pvName == null) pvName = req.getParameter("pv");

        if (pvName == null || pvName.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            infoValues = BAD_REQUEST;
            return infoValues;
        }

        boolean deleteData;
        String deleteDataStr = req.getParameter("deleteData");
        if (deleteDataStr != null && !deleteDataStr.isEmpty()) {
            deleteData = Boolean.parseBoolean(deleteDataStr);
        } else {
            deleteData = false;
        }

        String realName = configService.getRealNameForAlias(pvName);
        if (realName != null) {
            String finalPvName = pvName;
            logger.info(() -> "The name " + finalPvName + " is an alias for " + realName + ". Deleting this instead.");
            pvName = realName;
        }

        ApplianceInfo info = configService.getApplianceForPV(pvName);
        if (info == null) {
            String finalPvName1 = pvName;
            logger.debug(() -> "Unable to find appliance for PV " + finalPvName1);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            infoValues = BAD_REQUEST;
            return infoValues;
        }

        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.debug("Unable to find typeinfo for PV...");
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            infoValues = BAD_REQUEST;
            return infoValues;
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        if (!typeInfo.isPaused()) {
            infoValues =
                    "We will not stop archiving PV " + pvName + " if it is not paused. Please pause recording first.";
            logger.error(infoValues);
            return infoValues;
        }

        HashMap<String, String> pvStatus = new HashMap<String, String>();

        JSONObject pvEngineStatus = instanceDeletePV(
                pvStatus,
                "Engine start",
                info.getEngineURL() + "/deletePV"
                        + "?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&deleteData=" + deleteData,
                "Stopping archiving pv in engine using URL ",
                "Engine end");

        if (pvEngineStatus == null) {
            infoValues = "Unknown status from engine when stoppping archiving/deleting PV " + pvName + ".";
            logger.error(infoValues);
            return infoValues;
        }

        String finalPvName2 = pvName;
        logger.info(() -> "Stopping archiving for PV " + finalPvName2
                + (deleteData ? " and also deleting existing data" : " but keeping existing data"));

        JSONObject etlStatus = instanceDeletePV(
                pvStatus,
                "ETL start",
                info.getEtlURL() + "/deletePV"
                        + "?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&deleteData=" + Boolean.toString(deleteData),
                "Stopping archiving pv in ETL using URL ",
                "ETL end");
        GetUrlContent.combineJSONObjects(pvStatus, etlStatus);
        String finalPvName3 = pvName;
        logger.debug(() -> "Removing pv " + finalPvName3 + " from the cluster");
        pvStatus.put(
                "Start removing PV from cluster",
                TimeUtils.convertToHumanReadableString(System.currentTimeMillis() / 1000));
        configService.removePVFromCluster(pvName);
        pvStatus.put(
                "Done removing PV from cluster",
                TimeUtils.convertToHumanReadableString(System.currentTimeMillis() / 1000));

        String finalPvName4 = pvName;
        logger.debug(() -> "Removing aliases for pv " + finalPvName4 + " from the cluster");
        pvStatus.put(
                "Start removing aliases from cluster",
                TimeUtils.convertToHumanReadableString(System.currentTimeMillis() / 1000));
        List<String> aliases = configService.getAllAliases();
        for (String alias : aliases) {
            String realNameForAlias = configService.getRealNameForAlias(alias);
            if (pvName.equals(realNameForAlias)) {
                String finalPvName5 = pvName;
                logger.debug(() -> "Removing alias " + alias + " for pv " + finalPvName5);
                configService.removeAlias(alias, realNameForAlias);
            }
        }
        pvStatus.put(
                "Done removing aliases from cluster",
                TimeUtils.convertToHumanReadableString(System.currentTimeMillis() / 1000));

        return infoValues;
    }

    private void deleteMultiplePVs(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        StringBuilder strValues = new StringBuilder();
        LinkedList<String> pvNames = BulkPauseResumeUtils.getPVNames(req, configService);
        for (String pvName : pvNames) {
            strValues.append(deleteSinglePV(req, resp, configService, pvName)).append("\n");
        }

        HashMap<String, Object> infoValues = new HashMap<String, Object>();
        infoValues.put("validation", strValues.toString());
        if (strValues.toString().trim().isEmpty()) infoValues.put("status", "ok");
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(infoValues));
        }
    }
}
