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
import java.lang.reflect.Constructor;
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

        try {
            construct(actionClass, configService).execute(req, resp);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new IOException(e);
        }
    }

    /**
     * Build an action, supplying each constructor parameter from the configuration.
     * <p>An action declares the concerns it needs as constructor parameters; this resolves each one.
     * While a single object still implements every concern, resolution is an instance check against it.
     * Once the concerns have separate implementations this becomes a lookup in the component registry,
     * and at that point the method body is the only thing that changes.
     */
    static BPLAction construct(Class<? extends BPLAction> actionClass, ConfigService configService)
            throws ReflectiveOperationException {
        Constructor<?> ctor = actionClass.getDeclaredConstructors()[0];
        Class<?>[] wanted = ctor.getParameterTypes();
        Object[] args = new Object[wanted.length];
        for (int i = 0; i < wanted.length; i++) {
            if (!wanted[i].isInstance(configService)) {
                throw new IllegalStateException(actionClass.getName() + " asks for " + wanted[i].getName()
                        + ", which the configuration does not provide");
            }
            args[i] = configService;
        }
        ctor.setAccessible(true);
        return (BPLAction) ctor.newInstance(args);
    }

    /**
     * Check at startup that every registered action can actually be built, so a wiring mistake fails
     * on deployment rather than on the first request to that endpoint.
     * @param actions the actions registered by this webapp
     * @param configService the configuration they will be built from
     */
    public static void validateActions(Map<String, Class<? extends BPLAction>> actions, ConfigService configService) {
        for (Map.Entry<String, Class<? extends BPLAction>> e : actions.entrySet()) {
            Constructor<?> ctor = e.getValue().getDeclaredConstructors()[0];
            for (Class<?> wanted : ctor.getParameterTypes()) {
                if (!wanted.isInstance(configService)) {
                    throw new IllegalStateException("BPL action " + e.getValue().getName() + " registered at "
                            + e.getKey() + " asks for " + wanted.getName()
                            + ", which the configuration does not provide");
                }
            }
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
