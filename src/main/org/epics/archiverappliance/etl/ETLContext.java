/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * A class to hold state for one run of ETL.
 * A new ETL context is created for each run of ETL.
 * In addition, we also support the ability to execute code after completion of the entire run.
 * @author mshankar
 *
 */
public class ETLContext extends BasicContext {
    private static final Logger logger = LogManager.getLogger(ETLContext.class.getName());

    private final HashMap<String, Object> state = new HashMap<String, Object>();
    private final LinkedList<Runnable> postETLTasks = new LinkedList<Runnable>();

    public void put(String key, Object value) {
        state.put(key, value);
    }

    public Object get(String key) {
        return state.get(key);
    }

    public void executePostETLTasks() {
        for (Runnable runnable : postETLTasks) {
            try {
                runnable.run();
            } catch (Throwable t) {
                // We log but ignore throwables thrown by post ETL tasks.
                logger.error("Exception executing post ETL task", t);
            }
        }
    }
}
