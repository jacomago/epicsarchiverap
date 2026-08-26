package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.PVDirectory;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Gets the optimized aggregate typeInfo information for this appliance.
 * @author mshankar
 *
 */
public class AggregatedApplianceInfo implements BPLAction {

    private final ClusterTopology clusterTopology;
    private final PVDirectory pvdirectory;

    public AggregatedApplianceInfo(ClusterTopology clusterTopology, PVDirectory pvdirectory) {
        this.clusterTopology = clusterTopology;
        this.pvdirectory = pvdirectory;
    }

    private static Logger logger = LogManager.getLogger(AggregatedApplianceInfo.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        logger.debug("Getting the aggregated appliance information for the appliance"
                + clusterTopology.getMyApplianceInfo().getIdentity());

        ApplianceAggregateInfo aggregateInfo =
                pvdirectory.getAggregatedApplianceInfo(clusterTopology.getMyApplianceInfo());
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONEncoder<ApplianceAggregateInfo> jsonEncoder = JSONEncoder.getEncoder(ApplianceAggregateInfo.class);
            out.println(jsonEncoder.encode(aggregateInfo));
        } catch (Exception ex) {
            logger.error(
                    "ExceptionGetting the aggregated appliance information for the appliance"
                            + clusterTopology.getMyApplianceInfo().getIdentity(),
                    ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
