/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config.exception;

/**
 * Generic super class for all config service exceptions.
 * @author mshankar
 */
public class ConfigException extends Exception {
	private static final long serialVersionUID = -1195048537953477832L;
	/**
	 * Default constructor.
	 */
	public ConfigException() {
	}
	
	/**
	 * Constructor.
	 * @param msg Message
	 */
	public ConfigException(String msg) {
		super(msg);
	}
	/**
	 * Constructor.
	 * @param msg Message
	 * @param ex Cause
	 */
	public ConfigException(String msg, Throwable ex) {
		super(msg, ex);
	}
}
