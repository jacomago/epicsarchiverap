package org.epics.archiverappliance.common.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public interface Metrics extends BPLAction {

    /**
     * The configuration this report reads. An interface cannot hold the field, so the implementing
     * class supplies it — a record component of this name satisfies this automatically.
     * @return the configuration supplied to the implementor's constructor
     */
    ApplianceLifecycle configService();

    @Override
    default void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(metrics(configService())));
        }
    }

    Map<String, String> metrics(ApplianceLifecycle configService);
}
