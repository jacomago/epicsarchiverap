package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.AliasRegistry;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.PVTypeInfoStore;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Gets the type info for a PV as a JSON object
 *
 * @epics.BPLAction - Get the type info for a given PV. In the archiver appliance terminology, the PVTypeInfo contains the various archiving parameters for a PV.
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionEnd
 * @author mshankar
 *
 */
public class GetPVTypeInfo implements BPLAction {

    private final AliasRegistry aliasRegistry;
    private final PVTypeInfoStore pvtypeInfoStore;

    public GetPVTypeInfo(AliasRegistry aliasRegistry, PVTypeInfoStore pvtypeInfoStore) {
        this.aliasRegistry = aliasRegistry;
        this.pvtypeInfoStore = pvtypeInfoStore;
    }

    private static Logger logger = LogManager.getLogger(GetPVTypeInfo.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String pvName = req.getParameter("pv");
        logger.debug("Getting typeinfo for PV " + pvName);
        if (pvName == null || pvName.equals("")) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        // String pvNameFromRequest = pvName;
        String realName = aliasRegistry.getRealNameForAlias(pvName);
        if (realName != null) pvName = realName;

        PVTypeInfo typeInfo = pvtypeInfoStore.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            logger.warn("Cannot find typeinfo for " + pvName);
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONEncoder<PVTypeInfo> jsonEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
            out.println(jsonEncoder.encode(typeInfo));
        } catch (Exception ex) {
            logger.error("Exception marshalling typeinfo for pv " + pvName, ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
