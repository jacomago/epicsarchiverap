package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.PVDirectory;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Get a list of PVs for this appliance as a JSON array
 * @author mshankar
 *
 */
public class GetPVsForThisAppliance implements BPLAction {

    private final ClusterTopology clusterTopology;
    private final PVDirectory pvdirectory;

    public GetPVsForThisAppliance(ClusterTopology clusterTopology, PVDirectory pvdirectory) {
        this.clusterTopology = clusterTopology;
        this.pvdirectory = pvdirectory;
    }

    private static Logger logger = LogManager.getLogger(GetPVsForThisAppliance.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        logger.debug("Getting pvs for appliance "
                + clusterTopology.getMyApplianceInfo().getIdentity());
        LinkedList<String> pvsOnThisAppliance = new LinkedList<String>();
        for (String pvName : pvdirectory.getPVsForThisAppliance()) {
            pvsOnThisAppliance.add(pvName);
        }
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(pvsOnThisAppliance));
        } catch (Exception ex) {
            logger.error(
                    "Exception getting pvs for appliance "
                            + clusterTopology.getMyApplianceInfo().getIdentity(),
                    ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
