package org.epics.archiverappliance.common.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.CoreConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public interface MetricsDetails extends BPLAction {

    @Override
    default void execute(HttpServletRequest req, HttpServletResponse resp, CoreConfigService configService)
            throws IOException {

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(metricsDetails(configService)));
        }
    }

    LinkedList<Map<String, String>> metricsDetails(CoreConfigService configService);
}
