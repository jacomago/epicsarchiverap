/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import edu.stanford.slac.archiverappliance.PB.search.CompareEventLine;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A comparator for PB events that is used in searching.
 * @author mshankar
 *
 */
public class ComparePBEvent implements CompareEventLine {
	private final Instant yearSecondTimestamp;
	private final ArchDBRTypes type;
	private final short year;

	public ComparePBEvent(ArchDBRTypes type, Instant yearSecondTimestamp) {
		this.type = type;
		this.yearSecondTimestamp = yearSecondTimestamp;
		this.year = (short) ZonedDateTime.ofInstant(yearSecondTimestamp, ZoneId.of("Z")).getYear();
	}
	

	@Override
	public NextStep compare(byte[] line1, byte[] line2) throws IOException  {
		// The year does not matter here as we are driving solely off secondsintoyear. So we set it to 0.
		Instant line1Timestamp;
		Instant line2Timestamp = TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(
				year,
				PartitionGranularity.PARTITION_YEAR.getApproxSecondsPerChunk() + 1,
				0));
		try {
			// The raw forms for all the DBR types implement the PartionedTime interface
			Event e =
					Event.fromByteArray(type, new ByteArray(line1), year);
			line1Timestamp = e.instant();
			if(line2 != null) {
				Event e2 = Event.fromByteArray(type, new ByteArray(line2), year);
				line2Timestamp = e2.instant();
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
		if (line1Timestamp.compareTo(this.yearSecondTimestamp) > 0) {
			return NextStep.GO_LEFT;
		} else if (line2Timestamp.compareTo(this.yearSecondTimestamp) <= 0) {
			return NextStep.GO_RIGHT;
		} else {
			// If we are here, line1 < SS <= line2
			if(line2 != null) {
				if (line1Timestamp.compareTo(this.yearSecondTimestamp) <= 0
						&& line2Timestamp.compareTo(this.yearSecondTimestamp) > 0) {
					return NextStep.STAY_WHERE_YOU_ARE;
				} else {
					return NextStep.GO_LEFT;
				}
			} else {
				return NextStep.STAY_WHERE_YOU_ARE;
			}
		}
	}
}
