/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import java.io.IOException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * A very simple struts like action for business processes.
 * Responses are typically JSON though this is not enforced.
 * We are not too far away from the servlet container here.
 * A handle to the configuration is passed in as part of the execute method. The type parameter is
 * the slice of configuration the action actually needs, so each action declares its own; see
 * BasicDispatcher for how a mixed set of them is dispatched.
 * The BPLAction is extected to handle all servlet container traffic like HTTP error codes etc.
 * If an exception is thrown, the servlet that calls BPLActions will send a Internal Server Error to the caller.
 * @author mshankar
 *
 */
public interface BPLAction<C> {
    public void execute(HttpServletRequest req, HttpServletResponse resp, C configService) throws IOException;
}
