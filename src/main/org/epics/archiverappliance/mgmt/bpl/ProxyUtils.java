package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

import java.util.ArrayList;

/**
 * Small utility class to proxy mgmt BPL to appliance other than this appliance.
 * @author mshankar
 *
 */
public class ProxyUtils {
    private static Logger logger = LogManager.getLogger(ProxyUtils.class.getName());

    /**
     * Route pathAndQuery to all appliances other than this appliance
     * @param clusterTopology ConfigService
     * @param pathAndQuery  &emsp;
     */
    public static void routeURLToOtherAppliances(ClusterTopology clusterTopology, String pathAndQuery) {
        ArrayList<String> otherURLs = new ArrayList<String>();
        for (ApplianceInfo info : clusterTopology.getAppliancesInCluster()) {
            if (!info.equals(clusterTopology.getMyApplianceInfo())) {
                otherURLs.add(info.getMgmtURL() + pathAndQuery);
            }
        }

        for (String otherURL : otherURLs) {
            try {
                GetUrlContent.getURLContentAsJSONObject(otherURL);
            } catch (Throwable t) {
                logger.error("Exception getting content of URL " + otherURL, t);
            }
        }
    }
}
