package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.AppliancePVsView;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class StorageDetailsForAppliance implements BPLAction<AppliancePVsView> {

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, AppliancePVsView configService)
            throws IOException {
        try (PrintWriter out = resp.getWriter()) {
            out.println(StorageWithLifetime.getStorageDetails(configService));
        }
    }
}
