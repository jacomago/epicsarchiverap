/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceLifecycle;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Glue code that is in all the BPL servlets.
 *
 * @author mshankar
 */
public class BasicDispatcher {
    private static final Logger logger = LogManager.getLogger(BasicDispatcher.class);

    private BasicDispatcher() {}

    public static void dispatch(
            HttpServletRequest req,
            HttpServletResponse resp,
            ConfigService configService,
            Map<String, Class<? extends BPLAction>> actions)
            throws IOException {
        dispatch(req, resp, configService, actions, () -> true);
    }

    /**
     * Dispatch a BPL request, holding off the business actions until this webapp is ready to serve them.
     * The readiness check applies only to the business actions; /ping, /postStartup and /startupState always
     * run. The mgmt webapp gates on its children having started up, and the children reach mgmt through
     * /postStartup, so gating those would deadlock startup.
     * @param req &emsp;
     * @param resp &emsp;
     * @param configService &emsp;
     * @param actions The business actions registered by this webapp, keyed on request path.
     * @param webappReady False to reject business actions with a 500 until this webapp is ready.
     * @throws IOException &emsp;
     */
    public static void dispatch(
            HttpServletRequest req,
            HttpServletResponse resp,
            ConfigService configService,
            Map<String, Class<? extends BPLAction>> actions,
            BooleanSupplier webappReady)
            throws IOException {
        String requestPath = req.getPathInfo();
        if (requestPath == null || requestPath.equals("")) {
            logger.warn("Request path is empty.");
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        logger.info("Servicing " + requestPath);
        switch (requestPath) {
            case "/ping" -> ping(resp, "pong");
            case "/postStartup" -> postStartup(resp, configService);
            case "/startupState" -> startupState(resp, configService);
            default -> handleBPLAction(req, resp, configService, actions, requestPath, webappReady);
        }
    }

    private static void handleBPLAction(
            HttpServletRequest req,
            HttpServletResponse resp,
            ConfigService configService,
            Map<String, Class<? extends BPLAction>> actions,
            String requestPath,
            BooleanSupplier webappReady)
            throws IOException {
        if (!configService.isStartupComplete()) {
            logger.warn("We do not let the other actions complete until the config service startup is complete...");
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }

        if (!webappReady.getAsBoolean()) {
            String header = req.getHeader(GetUrlContent.ARCHAPPL_COMPONENT);
            if (header == null || !header.equals("true")) {
                logger.error("We do not let the actions complete until all the components have started up");
                resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
            }
        }

        Class<? extends BPLAction> actionClass = actions.get(requestPath);
        if (actionClass == null) {
            logger.error("Do not have a appropriate BPL action for " + requestPath
                    + ". Please register the appropriate business method in getActions.");
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        BPLAction action;
        try {
            action = actionClass.getConstructor().newInstance();
            action.execute(req, resp, configService);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new IOException(e);
        }
    }

    private static void startupState(HttpServletResponse resp, ApplianceLifecycle applianceLifecycle)
            throws IOException {
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            HashMap<String, String> ret = new HashMap<String, String>();
            ret.put("status", applianceLifecycle.getStartupState().toString());
            out.println(JSONObject.toJSONString(ret));
        }
    }

    private static void postStartup(HttpServletResponse resp, ApplianceLifecycle applianceLifecycle)
            throws IOException {
        if (applianceLifecycle.isStartupComplete()) {
            logger.warn("poststartup being called after startup complete");
        } else {
            try {
                applianceLifecycle.postStartup();
            } catch (ConfigException ex) {
                logger.fatal("Exception running postStartup", ex);
                throw new IOException(ex);
            }
        }
        ping(resp, "Done");
    }

    private static void ping(HttpServletResponse resp, String pong) throws IOException {
        resp.setContentType("text/plain");
        try (PrintWriter out = resp.getWriter()) {
            out.println(pong);
        }
    }
}
