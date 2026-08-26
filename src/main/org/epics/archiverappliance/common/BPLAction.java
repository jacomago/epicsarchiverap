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
 * The BPLAction is extected to handle all servlet container traffic like HTTP error codes etc.
 * If an exception is thrown, the servlet that calls BPLActions will send a Internal Server Error to the caller.
 *
 * <p>Configuration arrives through the <em>constructor</em>, not through execute. Declare a constructor
 * taking exactly the concern interfaces this action needs and {@link BasicDispatcher} supplies them;
 * an action needing nothing declares no constructor. An action needing six concerns therefore takes six
 * constructor parameters, and that is deliberate — it is the visible signal that the action does too much.
 *
 * @author mshankar
 *
 */
public interface BPLAction {
    public void execute(HttpServletRequest req, HttpServletResponse resp) throws IOException;
}
