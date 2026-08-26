package org.epics.archiverappliance.mgmt.policy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.PolicyService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GetPolicyList implements BPLAction {

    private final ClusterTopology clusterTopology;
    private final PolicyService policyService;

    public GetPolicyList(ClusterTopology clusterTopology, PolicyService policyService) {
        this.clusterTopology = clusterTopology;
        this.policyService = policyService;
    }

    private static Logger logger = LogManager.getLogger(GetPolicyList.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        logger.debug("Getting policies in this installation");
        HashMap<String, String> policies = policyService.getPoliciesInInstallation();
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(policies));
        } catch (Exception ex) {
            logger.error(
                    "Exception getting list of policies "
                            + clusterTopology.getMyApplianceInfo().getIdentity(),
                    ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
