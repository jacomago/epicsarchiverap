/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.SampleValue;

/**
 * Simple interface for a class that can be used to generate event streams based on some function. 
 * @author mshankar
 *
 */
public interface SimulationValueGenerator {

	/**
	 * Get the value at a particular point in time. 
	 * Many unit tests rely on the value being returned being absolutely reproducible given the time.
	 * @param type ArchDBRTypes  
	 * @param secondsIntoYear  &emsp; 
	 * @return Sample value  &emsp; 
	 */
	public SampleValue getSampleValue(ArchDBRTypes type, int secondsIntoYear) ;
}
