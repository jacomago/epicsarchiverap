package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.AppliancePVsView;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class StorageDetailsForAppliance implements BPLAction {

    private final AppliancePVsView configService;

    public StorageDetailsForAppliance(AppliancePVsView configService) {
        this.configService = configService;
    }

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try (PrintWriter out = resp.getWriter()) {
            out.println(StorageWithLifetime.getStorageDetails(configService));
        }
    }
}
