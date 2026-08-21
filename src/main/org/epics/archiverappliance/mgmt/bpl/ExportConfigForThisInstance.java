package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ClusterTopology;
import org.epics.archiverappliance.config.PVDirectory;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.PVTypeInfoStore;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Export the archiving configuration (PVTypeInfo's) for this instance as a JSON file.
 * Used for export and import of configuration.
 * @author mshankar
 *
 */
public class ExportConfigForThisInstance implements BPLAction {

    private final ClusterTopology clusterTopology;
    private final PVDirectory pvdirectory;
    private final PVTypeInfoStore pvtypeInfoStore;

    public ExportConfigForThisInstance(
            ClusterTopology clusterTopology, PVDirectory pvdirectory, PVTypeInfoStore pvtypeInfoStore) {
        this.clusterTopology = clusterTopology;
        this.pvdirectory = pvdirectory;
        this.pvtypeInfoStore = pvtypeInfoStore;
    }

    private static Logger logger = LogManager.getLogger(ExportConfigForThisInstance.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String identity = clusterTopology.getMyApplianceInfo().getIdentity();
        logger.info("Exporting config for this instance" + identity);

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println("[");
            JSONEncoder<PVTypeInfo> typeInfoEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
            boolean first = true;
            for (String pvName : pvdirectory.getPVsForThisAppliance()) {
                PVTypeInfo typeInfo = pvtypeInfoStore.getTypeInfoForPV(pvName);
                if (typeInfo != null) {
                    if (first) {
                        first = false;
                    } else {
                        out.println(",");
                    }
                    typeInfoEncoder.encodeAndPrint(typeInfo, out);
                } else {
                    logger.error("Not exporting configuration for pv " + pvName + " in appliance " + identity);
                }
            }
            out.println("]");
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
