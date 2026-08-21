package org.epics.archiverappliance.engine.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.PVDirectory;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class CurrentlyDisconnectedPVsAction implements BPLAction {

    private final ApplianceLifecycle applianceLifecycle;
    private final ClusterTopology clusterTopology;
    private final PVDirectory pvdirectory;

    public CurrentlyDisconnectedPVsAction(
            ApplianceLifecycle applianceLifecycle, ClusterTopology clusterTopology, PVDirectory pvdirectory) {
        this.applianceLifecycle = applianceLifecycle;
        this.clusterTopology = clusterTopology;
        this.pvdirectory = pvdirectory;
    }

    private static Logger logger = LogManager.getLogger(CurrentlyDisconnectedPVsAction.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String identity = clusterTopology.getMyApplianceInfo().getIdentity();
        logger.info("Currently disconnected PVs for appliance "
                + clusterTopology.getMyApplianceInfo().getIdentity());
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>();
        Set<String> pausedPVs = pvdirectory.getPausedPVsInThisAppliance();
        try (PrintWriter out = resp.getWriter()) {
            for (ArchiveChannel channel :
                    EngineContext.of(applianceLifecycle).getChannelList().values()) {
                PVMetrics pvMetrics = channel.getPVMetrics();
                if (!pvMetrics.isConnected()) {
                    String pvName = pvMetrics.getPvName();
                    if (pausedPVs.contains(pvName)) {
                        logger.debug("Skipping a paused PV " + pvName);
                        continue;
                    }
                    HashMap<String, String> pvStatus = new HashMap<String, String>();
                    result.add(pvStatus);
                    pvStatus.put("pvName", pvName);
                    pvStatus.put("instance", identity);
                    pvStatus.put(
                            "lastKnownEvent",
                            TimeUtils.convertToHumanReadableString(pvMetrics.getSecondsOfLastEvent()));
                    long connectionLastLostEpochSeconds = pvMetrics.getConnectionLastLostEpochSeconds();
                    pvStatus.put(
                            "connectionLostAt",
                            connectionLastLostEpochSeconds > 0
                                    ? TimeUtils.convertToHumanReadableString(connectionLastLostEpochSeconds)
                                    : TimeUtils.convertToHumanReadableString(
                                            applianceLifecycle.getTimeOfAppserverStartup()));
                    pvStatus.put(
                            "noConnectionAsOfEpochSecs",
                            Long.toString(
                                    connectionLastLostEpochSeconds > 0
                                            ? connectionLastLostEpochSeconds
                                            : applianceLifecycle.getTimeOfAppserverStartup()));
                    String hostName = channel.getHostName();
                    pvStatus.put("hostName", hostName != null ? hostName : "N/A");
                    pvStatus.put("commandThreadID", Integer.toString(channel.getJCACommandThreadID()));
                }
            }
            out.println(JSONValue.toJSONString(result));
        }
    }
}
