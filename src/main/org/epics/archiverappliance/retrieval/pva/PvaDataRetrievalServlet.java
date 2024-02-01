package org.epics.archiverappliance.retrieval.pva;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import javax.servlet.GenericServlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import static org.epics.archiverappliance.retrieval.pva.PvaDataRetrievalService.PVA_DATA_SERVICE;

public class PvaDataRetrievalServlet extends GenericServlet {

    private static final Logger logger = LogManager.getLogger(PvaDataRetrievalServlet.class.getName());

    /**
     *
     */
    private static final long serialVersionUID = 9178874095748814721L;

    private final PVAServer server;
    private ServerPV serverPV;

    public PvaDataRetrievalServlet() throws Exception {
        server = new PVAServer();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        ConfigService configService =
                (ConfigService) getServletConfig().getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
        serverPV = server.createPV(PVA_DATA_SERVICE, new PvaDataRetrievalService(configService));
        logger.info(ZonedDateTime.now(ZoneId.systemDefault()) + PVA_DATA_SERVICE + " is operational.");
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {}

    @Override
    public void destroy() {
        logger.info("Shutting down service " + PVA_DATA_SERVICE);
        serverPV.close();
        server.close();
        logger.info(PVA_DATA_SERVICE + " Shutdown complete.");
        super.destroy();
    }
}
