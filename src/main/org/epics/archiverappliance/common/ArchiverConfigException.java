/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;


/**
 * Use this to throw configuration related exceptions. 
 * @author mshankar
 *
 */
public class ArchiverConfigException extends Exception {
	/**
	 * Constructor with message and cause.
	 * @param arg0 Message
	 * @param arg1 Cause
	 */
	public ArchiverConfigException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	/**
	 * Constructor with message.
	 * @param arg0 Message
	 */
	public ArchiverConfigException(String arg0) {
		super(arg0);
	}

	private static final long serialVersionUID = -8779828706258704029L;

}
