package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.AliasRegistry;
import org.epics.archiverappliance.config.ClusterExecutor;
import org.epics.archiverappliance.config.PVDirectory;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Pause archiving the specified PV. This also tears down the CA channel for this PV.
 * @epics.BPLActionParam pv - The name of the pv. You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class PauseArchivingPV implements BPLAction {

    private final AliasRegistry aliasRegistry;
    private final PVDirectory pvDirectory;
    private final ClusterExecutor clusterExecutor;

    public PauseArchivingPV(AliasRegistry aliasRegistry, PVDirectory pvDirectory, ClusterExecutor clusterExecutor) {
        this.aliasRegistry = aliasRegistry;
        this.pvDirectory = pvDirectory;
        this.clusterExecutor = clusterExecutor;
    }

    private static Logger logger = LogManager.getLogger(PauseArchivingPV.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        if (req.getMethod().equals("POST")) {
            pauseMultiplePVs(req, resp);
            return;
        }

        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (pvName.contains(",") || pvName.contains("*") || pvName.contains("?")) {
            pauseMultiplePVs(req, resp);
        } else {
            // We only have one PV in the request
            List<String> pvNames = new LinkedList<String>();
            pvNames.add(pvName);
            pauseMultiplePVs(pvNames, resp);
        }
    }

    private void pauseMultiplePVs(HttpServletRequest req, HttpServletResponse resp)
            throws IOException, UnsupportedEncodingException {
        // String pvNameFromRequest = pvName;
        LinkedList<String> pvNames = BulkPauseResumeUtils.getPVNames(req, pvDirectory, aliasRegistry);
        pauseMultiplePVs(pvNames, resp);
    }

    private void pauseMultiplePVs(List<String> pvNames, HttpServletResponse resp)
            throws IOException, UnsupportedEncodingException {
        boolean askingToPausePV = true;
        List<HashMap<String, String>> response = BulkPauseResumeUtils.pauseResumePVs(
                pvNames, aliasRegistry, pvDirectory, clusterExecutor, askingToPausePV);
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

        if (pvNames.size() == 1) {
            try (PrintWriter out = resp.getWriter()) {
                out.println(JSONValue.toJSONString(response.getFirst()));
            }
            return;
        }

        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(response));
        }
    }
}
