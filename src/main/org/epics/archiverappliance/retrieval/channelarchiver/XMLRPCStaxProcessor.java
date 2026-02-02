/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import java.io.IOException;

/**
 * Our layer on top of STAX; similar to DefaultHandler with the exception of the boolean to indicate pause/resume processing.
 * @author mshankar
 *
 */
public interface XMLRPCStaxProcessor {
	
	/**
	 * Start element.
	 * @param localName Local name
	 * @return True to continue processing
	 * @throws IOException If processing fails
	 */
	public boolean startElement(String localName) throws IOException;

	/**
	 * End element.
	 * @param localName Local name
	 * @param value Value
	 * @return True to continue processing
	 * @throws IOException If processing fails
	 */
	public boolean endElement(String localName, String value) throws IOException;

}
