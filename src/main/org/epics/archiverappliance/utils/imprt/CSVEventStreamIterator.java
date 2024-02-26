/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.imprt;

import com.opencsv.CSVReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;

/**
 * The iterator for a CSV backed event stream
 * @author mshankar
 *
 */
class CSVEventStreamIterator implements Iterator<Event> {
	private static final Logger logger = LogManager.getLogger(CSVEventStreamIterator.class.getName());
	private final String csvFileName;
	private final ArchDBRTypes dbrtype;
	private final LineNumberReader linereader;
	private CSVReader csvreader;
	private Event nextEvent;

	public CSVEventStreamIterator(String fileName, ArchDBRTypes type) throws IOException {
		this.csvFileName = fileName;
		this.dbrtype = type;
		linereader = new LineNumberReader(new FileReader(fileName));
		csvreader = new CSVReader(linereader);
	}

	@Override
	public boolean hasNext() {
		nextEvent = readNextEvent();
        return nextEvent != null;
    }

	@Override
	public Event next() {
		return nextEvent;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("We do not support the remove method in this iterator");
	}

	public void close() {
		try { csvreader.close(); csvreader = null; } catch (Exception ignored) { }
	}
	
	private Event readNextEvent() {
		try { 
			String [] line = csvreader.readNext();
			if(line == null || line.length < 5) return null;
			return Event.fromCSV(line, dbrtype);
		} catch(Exception ex) {
			logger.error("Exception parsing CSV file " + csvFileName + " in line " + (linereader.getLineNumber()-1), ex);
		}
		return null;
	}
}